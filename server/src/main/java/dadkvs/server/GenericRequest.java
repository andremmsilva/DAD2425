package dadkvs.server;

import io.grpc.stub.StreamObserver;

public abstract class GenericRequest<T> {
    int reqId;
    StreamObserver<T> responseObserver;

    public GenericRequest(
            int reqId,
            StreamObserver<T> responseObserver) {
        this.reqId = reqId;
        this.responseObserver = responseObserver;
    }

    public abstract void process(DadkvsServerState serverState);
}
