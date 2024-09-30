package dadkvs.server;

import dadkvs.DadkvsMain;
import io.grpc.stub.StreamObserver;

public class WriteBufferableRequest extends BufferableRequest<DadkvsMain.CommitReply> {
    TransactionRecord record;

    public WriteBufferableRequest(int reqId, StreamObserver<DadkvsMain.CommitReply> responseObserver,
            TransactionRecord record) {
        super(reqId, responseObserver);
        this.record = record;
    }

    public WriteBufferableRequest(int reqId, StreamObserver<DadkvsMain.CommitReply> responseObserver,
            int sequence_number, TransactionRecord record) {
        super(reqId, responseObserver, sequence_number);
        this.record = record;
    }

    @Override
    public void process(DadkvsServerState serverState) {
        System.out.println("Executing write request");
        boolean result = serverState.store.commit(this.record);

        // for debug purposes
        System.out.println("Result is ready for request with reqid " + this.reqId);

        DadkvsMain.CommitReply response = DadkvsMain.CommitReply.newBuilder()
                .setReqid(this.reqId).setAck(result).build();

        this.responseObserver.onNext(response);
        this.responseObserver.onCompleted();
    }
}
