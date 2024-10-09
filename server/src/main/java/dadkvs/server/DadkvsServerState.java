package dadkvs.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class DadkvsServerState {
    boolean i_am_leader;
    int debug_mode;
    int base_port;
    int my_id;
    int store_size;
    KeyValueStore store;
    MainLoop main_loop;
    Thread main_loop_worker;

    List<Operation> operationLog = Collections.synchronizedList(new ArrayList<>());
    Map<Integer, GenericRequest<?>> pendingRequests = Collections.synchronizedMap(new HashMap<>());
    AtomicInteger lastExecutedIdx = new AtomicInteger(0);

    public DadkvsServerState(int kv_size, int port, int myself) {
        base_port = port;
        my_id = myself;
        i_am_leader = false;
        debug_mode = 0;
        store_size = kv_size;
        store = new KeyValueStore(kv_size);
        main_loop = new MainLoop(this);
        main_loop_worker = new Thread(main_loop);
        main_loop_worker.start();
    }

    public boolean isProposer() {
        return this.my_id <= 2;
    }

    public boolean isAcceptor() {
        return this.my_id <= 2;
    }
}
