package dadkvs.server;

import dadkvs.DadkvsMain;
import io.grpc.stub.StreamObserver;

public class CommitRequest extends GenericRequest<DadkvsMain.CommitReply> {
    TransactionRecord record;

    public CommitRequest(int reqId, StreamObserver<DadkvsMain.CommitReply> responseObserver,
            TransactionRecord record) {
        super(reqId, responseObserver);
        this.record = record;
    }

    @Override
    public void process(DadkvsServerState serverState) {
        boolean result = serverState.store.commit(this.record);

        // for debug purposes
        System.out.println("Result is ready for request with reqid " + this.reqId);

        DadkvsMain.CommitReply response = DadkvsMain.CommitReply.newBuilder()
                .setReqid(this.reqId).setAck(result).build();

        this.responseObserver.onNext(response);
        this.responseObserver.onCompleted();
    }
}