package dadkvs.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import dadkvs.DadkvsPaxosServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class DadkvsServerState {
    final int n_servers = 5;
    final String host = "localhost";
    boolean i_am_leader;
    int debug_mode;
    int base_port;
    int my_id;
    int store_size;
    KeyValueStore store;
    MainLoop main_loop;
    Thread main_loop_worker;
    PaxosHandler paxosHandler;

    List<Operation> operationLog = Collections.synchronizedList(new ArrayList<>());
    Map<Integer, GenericRequest<?>> pendingRequests = Collections.synchronizedMap(new HashMap<>());
    AtomicInteger lastExecutedIdx = new AtomicInteger(0);

    String[] targets;
    ManagedChannel[] channels;
    DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] async_stubs;

    public DadkvsServerState(int kv_size, int port, int myself) {
        base_port = port;
        my_id = myself;
        i_am_leader = false;
        debug_mode = 0;
        store_size = kv_size;
        store = new KeyValueStore(kv_size);
        makeStubs(port);
        main_loop = new MainLoop(this);
        paxosHandler = new PaxosHandler(this);
        main_loop_worker = new Thread(main_loop);
        main_loop_worker.start();
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

    public boolean isProposer() {
        return this.my_id <= 2;
    }

    public boolean isAcceptor() {
        return this.my_id <= 2;
    }
}
