package dadkvs.server;

import dadkvs.DadkvsPaxos;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import java.util.*;

public class PaxosProcessor extends Thread {
    private DadkvsServerState serverState;
    private int proposalIdx;
    private int proposalTs;
    private int proposalVal;

    private ArrayList<DadkvsPaxos.PhaseOneReply> phaseOneReplies = new ArrayList<>();
    private ArrayList<DadkvsPaxos.PhaseTwoReply> phaseTwoReplies = new ArrayList<>();
    private ArrayList<DadkvsPaxos.LearnReply> learnReplies = new ArrayList<>();

    public PaxosProcessor(DadkvsServerState serverState) {
        this.serverState = serverState;
        this.proposalIdx = -1;
        this.proposalVal = -1;
        this.proposalTs = 0;
    }

    private int getAcceptorId(int idx) {
        return idx + serverState.store.read(0).getValue();
    }

    private int generateProposalNum(int lastTs) {
        final int round = lastTs / serverState.n_proposers;
        return (round + 1) * serverState.n_proposers + serverState.my_id;
    }

    private void applyDelay() {
        if (serverState.debug_mode == 6) {
            // Slow paxos
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void waitForLeaderStatus() throws InterruptedException {
        try {
            serverState.leaderLock.lock();
            while (!serverState.i_am_leader) {
                System.out.println("Waiting for leader state");
                serverState.leaderCond.await();
            }
        } finally {
            serverState.leaderLock.unlock();
        }
    }

    private void waitForReconfiguration() throws InterruptedException {
        try {
            serverState.reconfigLock.lock();
            while (serverState.isReconfigPending) {
                System.out.println("Waiting for reconfiguration to be over");
                serverState.reconfigCond.await();
            }
        } finally {
            serverState.reconfigLock.unlock();
        }
    }

    private void sendProposal() {
        System.out.println(
                "Proposing reqId " + this.proposalVal + " for index " + proposalIdx + " with ts "
                        + proposalTs);
        applyDelay();

        DadkvsPaxos.PhaseOneRequest.Builder phaseOneReq = DadkvsPaxos.PhaseOneRequest.newBuilder()
                .setPhase1Config(0)
                .setPhase1Index(this.proposalIdx)
                .setPhase1Timestamp(this.proposalTs);

        phaseOneReplies.clear();
        GenericResponseCollector<DadkvsPaxos.PhaseOneReply> collector = new GenericResponseCollector<>(
                phaseOneReplies, serverState.n_acceptors);

        for (int i = 0; i < serverState.n_acceptors; i++) {
            int acceptorId = this.getAcceptorId(i);
            CollectorStreamObserver<DadkvsPaxos.PhaseOneReply> responseObserver = new CollectorStreamObserver<>(
                    collector);
            serverState.async_stubs[acceptorId].phaseone(phaseOneReq.build(),
                    responseObserver);
        }

        // Wait for all replies, consider only a majority
        collector.waitForTarget(serverState.n_acceptors);
    }

    private void sendAcceptReq() {
        System.out.println(
                "Sending accept for reqId " + this.proposalVal + " for index " + proposalIdx + " with ts "
                        + proposalTs);
        applyDelay();

        DadkvsPaxos.PhaseTwoRequest.Builder phaseTwoReq = DadkvsPaxos.PhaseTwoRequest.newBuilder()
                .setPhase2Config(0)
                .setPhase2Index(this.proposalIdx)
                .setPhase2Timestamp(this.proposalTs)
                .setPhase2Value(this.proposalVal);

        phaseTwoReplies.clear();
        GenericResponseCollector<DadkvsPaxos.PhaseTwoReply> collector = new GenericResponseCollector<>(
                phaseTwoReplies, serverState.n_acceptors);

        for (int i = 0; i < serverState.n_acceptors; i++) {
            int acceptorId = this.getAcceptorId(i);
            CollectorStreamObserver<DadkvsPaxos.PhaseTwoReply> responseObserver = new CollectorStreamObserver<>(
                    collector);
            serverState.async_stubs[acceptorId].phasetwo(phaseTwoReq.build(),
                    responseObserver);
        }

        // Wait for all replies, consider only a majority
        collector.waitForTarget(serverState.n_acceptors);
    }

    private void sendLearn() {
        System.out.println(
                "Sending learn request for reqId " + this.proposalVal + " for index " + proposalIdx + " with ts "
                        + proposalTs);
        applyDelay();

        DadkvsPaxos.LearnRequest.Builder learnReq = DadkvsPaxos.LearnRequest.newBuilder()
                .setLearnconfig(0)
                .setLearnindex(this.proposalIdx)
                .setLearntimestamp(this.proposalTs)
                .setLearnvalue(this.proposalVal);

        learnReplies.clear();
        GenericResponseCollector<DadkvsPaxos.LearnReply> collector = new GenericResponseCollector<>(
                learnReplies, serverState.n_servers);

        for (int i = 0; i < serverState.n_servers; i++) {
            CollectorStreamObserver<DadkvsPaxos.LearnReply> responseObserver = new CollectorStreamObserver<>(
                    collector);
            serverState.async_stubs[i].learn(learnReq.build(), responseObserver);
        }

        // Wait for all replies, consider only a majority
        collector.waitForTarget(serverState.n_servers);
    }

    private void runConsensus() throws InterruptedException {
        final int majority = (serverState.n_acceptors / 2) + 1;

        // Wait unitl we are the leader
        waitForLeaderStatus();

        // Wait until pending reconfigurations are over
        waitForReconfiguration();

        // Wait until a proposal is available
        this.proposalVal = serverState.proposalQueue.take();
        if (serverState.learnedReqIds.contains(this.proposalVal)) {
            System.out.println("Discarding proposal for reqId " + this.proposalVal);
            // This consensus has already finished, just remove it from the queue
            return;
        }

        this.proposalIdx = serverState.nextPromiseIdx.get();
        boolean done = false;

        // Phase one (repeat until we have a majority)
        while (!done) {
            int nAccepted = 0;
            this.proposalTs = generateProposalNum(this.proposalTs);

            this.sendProposal();

            int highestRepliedTs = 0;
            for (DadkvsPaxos.PhaseOneReply reply : phaseOneReplies) {
                System.out.println("received phase 1 reply with ack " + reply.getPhase1Accepted() + " ts "
                        + reply.getPhase1Timestamp() + " val " + reply.getPhase1Value() + " index "
                        + reply.getPhase1Index());
                if (reply.getPhase1Accepted()) {
                    nAccepted++;
                    if (reply.getPhase1Value() != -1) {
                        if (reply.getPhase1Timestamp() > highestRepliedTs) {
                            highestRepliedTs = reply.getPhase1Timestamp();
                            this.proposalVal = reply.getPhase1Value();
                        }
                    }
                }
            }

            if (nAccepted >= majority) {
                // We have a majority of promises, lets send the phase two
                nAccepted = 0;
                this.sendAcceptReq();

                for (DadkvsPaxos.PhaseTwoReply reply : phaseTwoReplies) {
                    System.out
                            .println("received phase 2 reply with ack " + reply.getPhase2Accepted()
                                    + " index "
                                    + reply.getPhase2Index());

                    if (reply.getPhase2Accepted()) {
                        nAccepted++;
                    }
                }

                if (nAccepted >= majority) {
                    done = true;
                }
            }
        }

        // Learn phase
        this.sendLearn();
    }

    @Override
    public void run() {
        try {
            while (true) {
                runConsensus();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
