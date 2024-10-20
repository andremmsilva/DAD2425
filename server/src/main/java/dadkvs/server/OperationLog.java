package dadkvs.server;

import java.util.*;

public class OperationLog extends AbstractList<Operation> {
    private List<Operation> internalList = Collections.synchronizedList(new ArrayList<>());

    public OperationLog() {
    }

    public OperationLog(int initialSize) {
        ensureCapacity(initialSize);
    }

    private void ensureCapacity(int lastIdx) {
        synchronized (internalList) {
            while (lastIdx >= internalList.size()) {
                internalList.add(null);
            }
        }
    }

    @Override
    public Operation set(int index, Operation operation) {
        synchronized (internalList) {
            ensureCapacity(index);
            return internalList.set(index, operation);
        }
    }

    @Override
    public boolean add(Operation operation) {
        synchronized (internalList) {
            return internalList.add(operation);
        }
    }

    @Override
    public void add(int index, Operation operation) {
        synchronized (internalList) {
            ensureCapacity(index);
            internalList.add(index, operation);
        }
    }

    @Override
    public int size() {
        synchronized (internalList) {
            return internalList.size();
        }
    }

    @Override
    public Operation get(int index) {
        synchronized (internalList) {
            if (index >= internalList.size()) {
                return null;
            }

            return internalList.get(index);
        }
    }
}
