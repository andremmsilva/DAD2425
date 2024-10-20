package dadkvs.server;

import dadkvs.DadkvsMain;
import io.grpc.stub.StreamObserver;

public class ReadRequest extends GenericRequest<DadkvsMain.ReadReply> {
    int key;

    public ReadRequest(
            int reqId,
            StreamObserver<DadkvsMain.ReadReply> responseObserver,
            int key) {
        super(reqId, responseObserver);
        this.key = key;
    }

    @Override
    public void process(DadkvsServerState serverState) {
        // Perform the read operation
        System.out.println("Executing read request");
        VersionedValue value = serverState.store.read(this.key);

        // Build and send the response for the read request
        DadkvsMain.ReadReply response = DadkvsMain.ReadReply.newBuilder()
                .setReqid(this.reqId)
                .setValue(value.getValue()) // Assuming read results in a value
                .setTimestamp(value.getVersion()) // Include the timestamp/version of the value
                .build();
        this.responseObserver.onNext(response);
        this.responseObserver.onCompleted();
    }
}