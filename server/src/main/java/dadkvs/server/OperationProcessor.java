package dadkvs.server;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class OperationProcessor extends Thread {
    private DadkvsServerState serverState;
    private final Lock lock = new ReentrantLock();
    private final Condition operationChangedCondition = lock.newCondition();

    public OperationProcessor(DadkvsServerState serverState) {
        this.serverState = serverState;
    }

    public void signalOperation() {
        lock.lock();
        try {
            operationChangedCondition.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void run() {
        while (true) {
            lock.lock();
            try {
                while (serverState.operationLog.size() <= serverState.nextExecuteIdx.get()
                        || serverState.operationLog.get(serverState.nextExecuteIdx.get()) == null) {
                    operationChangedCondition.await();
                }

                Operation op = serverState.operationLog.get(serverState.nextExecuteIdx.get());
                if (op != null) {
                    System.out.println(
                            "Running operation with index " + op.getIndex() + " value " + op.getAcceptedReqId());
                    op.execute();
                    serverState.nextExecuteIdx.incrementAndGet();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            } finally {
                lock.unlock();
            }
        }
    }
}
