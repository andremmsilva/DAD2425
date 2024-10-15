package dadkvs.server;

import java.util.ArrayList;

import dadkvs.DadkvsPaxos;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;

public class PaxosHandler {
    DadkvsServerState serverState;
    Operation currOperation;

    private int generateProposalNum() {
        final int counter = currOperation.promisedNum / serverState.n_servers + 1;
        return serverState.my_id + counter * serverState.n_servers;
    }

    public PaxosHandler(DadkvsServerState serverState) {
        this.serverState = serverState;
        this.currOperation = null;
    }

    public void propose(int reqId) {
        final int majority = serverState.n_servers / 2;
        if (currOperation != null)
            return;

        // Fill in operation data
        currOperation = new Operation();
        currOperation.reqId = reqId;
        currOperation.promisedNum = generateProposalNum();

        // Propose for our next index in the log
        final int index = serverState.operationLog.size();
        DadkvsPaxos.PhaseOneRequest.Builder phaseOneReq = DadkvsPaxos.PhaseOneRequest.newBuilder()
                .setPhase1Config(0)
                .setPhase1Index(index)
                .setPhase1Timestamp(currOperation.promisedNum);

        ArrayList<DadkvsPaxos.PhaseOneReply> phaseOneReplies = new ArrayList<>();
        GenericResponseCollector<DadkvsPaxos.PhaseOneReply> collector = new GenericResponseCollector<>(
                phaseOneReplies, serverState.n_servers);

        System.out.println("Proposed operation for index " + index + " with proposalNum " + currOperation.promisedNum);
        for (int i = 0; i < serverState.n_servers; i++) {
            if (i == serverState.my_id)
                continue;

            CollectorStreamObserver<DadkvsPaxos.PhaseOneReply> responseObserver = new CollectorStreamObserver<>(
                    collector);
            serverState.async_stubs[i].phaseone(phaseOneReq.build(), responseObserver);
        }

        // Wait for all replies, consider only a majority
        collector.waitForTarget(serverState.n_servers - 1);
        int n_accepted = 0;

        for (DadkvsPaxos.PhaseOneReply reply : phaseOneReplies) {
            if (reply.getPhase1Accepted()) {
                n_accepted++;
            }
        }

        if (n_accepted > majority) {
            this.sendAccept();
        }

        // TODO - What if not accepted?
    }

    public void sendAccept() {

    }
}
