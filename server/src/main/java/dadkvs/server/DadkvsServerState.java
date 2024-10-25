package dadkvs.server;

import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import dadkvs.DadkvsPaxosServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class DadkvsServerState {
    final int n_proposers = 3;
    final int n_acceptors = 3;
    final int n_servers = 5;
    final String host = "localhost";

    boolean i_am_leader;
    Lock leaderLock = new ReentrantLock();
    Condition leaderCond = leaderLock.newCondition();

    int debug_mode;
    int base_port;
    int my_id;
    int store_size;
    KeyValueStore store;
    String[] targets;
    ManagedChannel[] channels;
    DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] async_stubs;

    OperationLog operationLog = new OperationLog(1000);
    BlockingQueue<Integer> proposalQueue = new LinkedBlockingQueue<>();
    ConcurrentHashMap<Integer, GenericRequest<?>> requestMap = new ConcurrentHashMap<>();
    Set<Integer> learnedReqIds = ConcurrentHashMap.newKeySet();

    Lock newReqLock = new ReentrantLock();
    Condition newReqCondition = newReqLock.newCondition();

    Lock reconfigLock = new ReentrantLock();
    Condition reconfigCond = reconfigLock.newCondition();
    boolean isReconfigPending = false;

    AtomicInteger nextPromiseIdx = new AtomicInteger(0);
    AtomicInteger nextExecuteIdx = new AtomicInteger(0);

    PaxosProcessor paxosProcessor;
    OperationProcessor operationProcessor;

    public DadkvsServerState(int kv_size, int port, int myself) {
        base_port = port;
        my_id = myself;
        i_am_leader = false;
        debug_mode = 0;
        store_size = kv_size;
        store = new KeyValueStore(kv_size);
        makeStubs(port);
        paxosProcessor = new PaxosProcessor(this);
        operationProcessor = new OperationProcessor(this);
        paxosProcessor.start();
        operationProcessor.start();
    }

    private void terminateComms() {
        for (int i = 0; i < n_servers; i++) {
            channels[i].shutdownNow();
        }
    }

    public void crashServer() {
        System.out.println("Crashing the server...");
        this.terminateComms();
        paxosProcessor.interrupt();
        operationProcessor.interrupt();
        System.exit(1);
    }

    public boolean isInConfiguration(int configVal) {
        return this.my_id >= configVal && this.my_id < configVal + 3;
    }

    public void makeStubs(int port) {
        targets = new String[n_servers];
        for (int i = 0; i < n_servers; i++) {
            int target_port = port + i;
            targets[i] = new String();
            targets[i] = host + ":" + target_port;
            System.out.printf("targets[%d] = %s%n", i, targets[i]);
        }

        // Let us use plaintext communication because we do not have certificates
        channels = new ManagedChannel[n_servers];
        for (int i = 0; i < n_servers; i++) {
            channels[i] = ManagedChannelBuilder.forTarget(targets[i]).usePlaintext().build();
        }

        async_stubs = new DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[n_servers];
        for (int i = 0; i < n_servers; i++) {
            async_stubs[i] = DadkvsPaxosServiceGrpc.newStub(channels[i]);
        }
    }

    public void addNewRequest(int reqId, GenericRequest<?> req) {
        newReqLock.lock();
        try {
            requestMap.put(reqId, req);
            newReqCondition.signal();
        } finally {
            newReqLock.unlock();
        }
    }
}
