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

    private int generateProposalNum(int lastTs) {
        final int round = lastTs / serverState.n_proposers;
        return (round + 1) * serverState.n_proposers + serverState.my_id;
    }

    private void sendProposal() {
        System.out.println(
                "Proposing reqId " + this.proposalVal + " for index " + proposalIdx + " with ts "
                        + proposalTs);

        DadkvsPaxos.PhaseOneRequest.Builder phaseOneReq = DadkvsPaxos.PhaseOneRequest.newBuilder()
                .setPhase1Config(0)
                .setPhase1Index(this.proposalIdx)
                .setPhase1Timestamp(this.proposalTs);

        phaseOneReplies.clear();
        GenericResponseCollector<DadkvsPaxos.PhaseOneReply> collector = new GenericResponseCollector<>(
                phaseOneReplies, serverState.n_acceptors);

        // #TODO send to the right acceptors
        for (int i = 0; i < serverState.n_acceptors; i++) {
            CollectorStreamObserver<DadkvsPaxos.PhaseOneReply> responseObserver = new CollectorStreamObserver<>(
                    collector);
            serverState.async_stubs[i + serverState.store.read(0).getValue()].phaseone(phaseOneReq.build(), responseObserver);
        }

        // Wait for all replies, consider only a majority
        collector.waitForTarget(serverState.n_acceptors);
    }

    private void sendAcceptReq() {
        System.out.println(
                "Sending accept for reqId " + this.proposalVal + " for index " + proposalIdx + " with ts "
                        + proposalTs);

        DadkvsPaxos.PhaseTwoRequest.Builder phaseTwoReq = DadkvsPaxos.PhaseTwoRequest.newBuilder()
                .setPhase2Config(0)
                .setPhase2Index(this.proposalIdx)
                .setPhase2Timestamp(this.proposalTs)
                .setPhase2Value(this.proposalVal);

        phaseTwoReplies.clear();
        GenericResponseCollector<DadkvsPaxos.PhaseTwoReply> collector = new GenericResponseCollector<>(
                phaseTwoReplies, serverState.n_acceptors);

        // #TODO send to the right acceptors
        for (int i = 0; i < serverState.n_acceptors; i++) {
            CollectorStreamObserver<DadkvsPaxos.PhaseTwoReply> responseObserver = new CollectorStreamObserver<>(
                    collector);
            serverState.async_stubs[i + serverState.store.read(0).getValue()].phasetwo(phaseTwoReq.build(), responseObserver);
        }

        // Wait for all replies, consider only a majority
        collector.waitForTarget(serverState.n_acceptors);
    }

    private void sendLearn() {
        System.out.println(
                "Sending learn request for reqId " + this.proposalVal + " for index " + proposalIdx + " with ts "
                        + proposalTs);

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

    @Override
    public void run() {
        final int majority = (serverState.n_acceptors / 2) + 1;
        try {
            while (true) {
                // Wait until a proposal is available
                this.proposalVal = serverState.proposalQueue.take();
                if (!serverState.i_am_leader) {
                    continue;
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
                                    .println("received phase 2 reply with ack " + reply.getPhase2Accepted() + " index "
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
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
