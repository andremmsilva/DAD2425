package dadkvs.server;

public class Operation {
    int promisedId;
    int acceptedId;
    int acceptedReqId;
    boolean canExecute;

    public Operation() {
        this.promisedId = 0;
        this.acceptedId = 0;
        this.acceptedReqId = -1;
        this.canExecute = false;
    }

    public Operation(int promisedId) {
        this.promisedId = promisedId;
        this.acceptedId = 0;
        this.acceptedReqId = -1;
        this.canExecute = false;
    }
}
