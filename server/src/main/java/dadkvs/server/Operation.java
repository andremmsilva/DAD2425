package dadkvs.server;

public class Operation {
    private int index;
    private int promisedTs;
    private int acceptedTs;
    private int acceptedReqId;
    DadkvsServerState serverState;

    public Operation(DadkvsServerState serverState, int index) {
        this.serverState = serverState;
        this.index = index;
        this.promisedTs = 0;
        this.acceptedTs = 0;
        this.acceptedReqId = -1;
    }

    public void execute() throws InterruptedException {
        serverState.newReqLock.lock();
        GenericRequest<?> req = serverState.requestMap.get(this.acceptedReqId);
        try {
            while (req == null) {
                serverState.newReqCondition.await();
                req = serverState.requestMap.get(this.acceptedReqId);
            }
        } finally {
            serverState.newReqLock.unlock();
        }

        if (req != null) {
            req.process(serverState);
        }
    }

    public synchronized int getIndex() {
        return this.index;
    }

    public synchronized void setIndex(int index) {
        this.index = index;
    }

    public synchronized int getPromisedTs() {
        return this.promisedTs;
    }

    public synchronized void setPromisedTs(int promisedTs) {
        this.promisedTs = promisedTs;
    }

    public synchronized int getAcceptedTs() {
        return this.acceptedTs;
    }

    public synchronized void setAcceptedTs(int acceptedTs) {
        this.acceptedTs = acceptedTs;
    }

    public synchronized int getAcceptedReqId() {
        return this.acceptedReqId;
    }

    public synchronized void setAcceptedReqId(int acceptedReqId) {
        this.acceptedReqId = acceptedReqId;
    }
}
