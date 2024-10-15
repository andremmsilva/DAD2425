package dadkvs.server;

public class Operation {
    int promisedNum;
    int acceptedNum;
    int reqId;
    boolean canExecute;

    public Operation() {
        this.promisedNum = 0;
        this.acceptedNum = 0;
        this.reqId = -1;
        this.canExecute = false;
    }

    public Operation(int promisedNum) {
        this.promisedNum = promisedNum;
        this.acceptedNum = 0;
        this.reqId = -1;
        this.canExecute = false;
    }
}
