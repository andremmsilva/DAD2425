package dadkvs.server;

import io.grpc.stub.StreamObserver;

public abstract class BufferableRequest<T> implements Comparable<BufferableRequest<?>> {
    int reqId;
    int sequenceNumber;
    StreamObserver<T> responseObserver;

    public BufferableRequest(
            int reqId,
            StreamObserver<T> responseObserver) {
        this.reqId = reqId;
        this.sequenceNumber = -1;
        this.responseObserver = responseObserver;
    }

    public BufferableRequest(
            int reqId,
            StreamObserver<T> responseObserver,
            int sequence_number) {
        this.reqId = reqId;
        this.sequenceNumber = sequence_number;
        this.responseObserver = responseObserver;
    }

    public synchronized void setSequenceNumber(int sequence_number) {
        this.sequenceNumber = sequence_number;
    }

    @Override
    public int compareTo(BufferableRequest<?> o) {
        // For the priority queue
        return this.sequenceNumber - o.sequenceNumber;
    }

    public abstract void process(DadkvsServerState serverState);
}
