package dadkvs.server;

import dadkvs.DadkvsMain;
import io.grpc.stub.StreamObserver;

public class WriteRequest extends GenericRequest<DadkvsMain.CommitReply> {
    int key1;
    int version1;
    int key2;
    int version2;
    int writekey;
    int writeval;
    int timestamp;

    public WriteRequest(
            int reqId,
            StreamObserver<DadkvsMain.CommitReply> responseObserver,
            int key1,
            int version1,
            int key2,
            int version2,
            int writekey,
            int writeval,
            int timestamp) {
        super(reqId, responseObserver);
        this.key1 = key1;
        this.version1 = version1;
        this.key2 = key2;
        this.version2 = version2;
        this.writekey = writekey;
        this.writeval = writeval;
        this.timestamp = timestamp;
    }

    @Override
    public void process(DadkvsServerState serverState) {
        TransactionRecord txrecord = new TransactionRecord(key1, version1, key2, version2, writekey, writeval,
                timestamp);
        boolean result = serverState.store.commit(txrecord);

        // for debug purposes
        System.out.println("Processed request with reqid " + reqId);

        DadkvsMain.CommitReply response = DadkvsMain.CommitReply.newBuilder()
                .setReqid(reqId).setAck(result).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
