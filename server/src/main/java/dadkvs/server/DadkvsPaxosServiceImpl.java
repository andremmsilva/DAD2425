
package dadkvs.server;

import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import io.grpc.stub.StreamObserver;

public class DadkvsPaxosServiceImpl extends DadkvsPaxosServiceGrpc.DadkvsPaxosServiceImplBase {

    DadkvsServerState server_state;

    public DadkvsPaxosServiceImpl(DadkvsServerState state) {
        this.server_state = state;

    }

    @Override
    public void phaseone(DadkvsPaxos.PhaseOneRequest request,
            StreamObserver<DadkvsPaxos.PhaseOneReply> responseObserver) {
        // for debug purposes
        int index = request.getPhase1Index();
        int timestamp = request.getPhase1Timestamp();
        System.out.println("Receive phase1 request for index " + index + " ts " + timestamp);
        DadkvsPaxos.PhaseOneReply.Builder phaseOneReply = DadkvsPaxos.PhaseOneReply.newBuilder().setPhase1Config(0);

        Operation o = server_state.operationLog.get(index);
        if (o == null) {
            o = new Operation(server_state, index);
            server_state.operationLog.set(index, o);
        }

        if (o.getPromisedTs() >= timestamp) {
            System.out.println("Rejecting proposal with ts " + timestamp);
            phaseOneReply
                    .setPhase1Accepted(false)
                    .setPhase1Index(index)
                    .setPhase1Timestamp(o.getAcceptedTs())
                    .setPhase1Value(o.getAcceptedReqId());
        } else {
            System.out.println("Accepting proposal with ts " + timestamp);
            o.setPromisedTs(timestamp);
            phaseOneReply
                    .setPhase1Accepted(true)
                    .setPhase1Index(index)
                    .setPhase1Timestamp(o.getAcceptedTs())
                    .setPhase1Value(o.getAcceptedReqId());
        }
        responseObserver.onNext(phaseOneReply.build());
        responseObserver.onCompleted();
    }

    @Override
    public void phasetwo(DadkvsPaxos.PhaseTwoRequest request,
            StreamObserver<DadkvsPaxos.PhaseTwoReply> responseObserver) {
        // for debug purposes
        int index = request.getPhase2Index();
        int timestamp = request.getPhase2Timestamp();
        int value = request.getPhase2Value();
        System.out.println("Receive phase two request for index " + index + " ts " + timestamp + " val " + value);
        DadkvsPaxos.PhaseTwoReply.Builder phaseTwoReply = DadkvsPaxos.PhaseTwoReply.newBuilder().setPhase2Config(0);
        Operation o = server_state.operationLog.get(index);
        if (o == null) {
            // Shouldn't happen
            o = new Operation(server_state, index);
            server_state.operationLog.set(index, o);
        }

        if (o.getPromisedTs() > timestamp) {
            System.out.println("Rejecting accept with ts " + timestamp);
            phaseTwoReply
                    .setPhase2Accepted(false)
                    .setPhase2Index(index);
        } else {
            o.setAcceptedReqId(value);
            o.setAcceptedTs(timestamp);
            o.setPromisedTs(timestamp);
            phaseTwoReply.setPhase2Accepted(true).setPhase2Index(index);
        }

        responseObserver.onNext(phaseTwoReply.build());
        responseObserver.onCompleted();
    }

    @Override
    public void learn(DadkvsPaxos.LearnRequest request, StreamObserver<DadkvsPaxos.LearnReply> responseObserver) {
        // for debug purposes
        int index = request.getLearnindex();
        int timestamp = request.getLearntimestamp();
        int value = request.getLearnvalue();
        System.out.println("Receive learn request for index " + index + " ts " + timestamp + " val " + value);
        DadkvsPaxos.LearnReply.Builder learnReply = DadkvsPaxos.LearnReply.newBuilder()
                .setLearnaccepted(true)
                .setLearnconfig(0)
                .setLearnindex(index);

        Operation o = server_state.operationLog.get(index);
        if (o == null) {
            o = new Operation(server_state, index);
        }
        o.setAcceptedReqId(value);
        o.setAcceptedTs(timestamp);
        o.setPromisedTs(timestamp);
        server_state.operationLog.set(index, o);
        server_state.operationProcesssor.signalOperation();
        server_state.nextPromiseIdx.set(index + 1);

        responseObserver.onNext(learnReply.build());
        responseObserver.onCompleted();
    }

}
