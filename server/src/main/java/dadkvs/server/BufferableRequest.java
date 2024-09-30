package dadkvs.server;

import dadkvs.DadkvsMain;
import io.grpc.stub.StreamObserver;

public class BufferableRequest implements Comparable<BufferableRequest> {
    int reqId;
    int sequenceNumber;
    StreamObserver<DadkvsMain.CommitReply> responseObserver;
    TransactionRecord record;

    public BufferableRequest(
            int reqId,
            StreamObserver<DadkvsMain.CommitReply> responseObserver,
            TransactionRecord record) {
        this.reqId = reqId;
        this.sequenceNumber = -1;
        this.record = record;
        this.responseObserver = responseObserver;
    }

    public BufferableRequest(
            int reqId,
            StreamObserver<DadkvsMain.CommitReply> responseObserver,
            TransactionRecord record,
            int sequence_number) {
        this.reqId = reqId;
        this.sequenceNumber = sequence_number;
        this.record = record;
        this.responseObserver = responseObserver;
    }

    public synchronized void setSequenceNumber(int sequence_number) {
        this.sequenceNumber = sequence_number;
    }

    @Override
    public int compareTo(BufferableRequest o) {
        // For the priority queue
        return this.sequenceNumber - o.sequenceNumber;
    }
}
