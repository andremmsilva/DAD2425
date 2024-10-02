package dadkvs.server;

import dadkvs.DadkvsMainServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.*;

public class DadkvsServerState {
    final int n_servers = 5;
    final String host = "localhost";

    int configuration;
    boolean i_am_leader;
    int debug_mode;
    int base_port;
    int my_id;
    int store_size;
    int sequence_number;
    KeyValueStore store;
    MainLoop main_loop;
    Thread main_loop_worker;

    Queue<BufferableRequest> workToDo = new PriorityQueue<>();
    Map<Integer, BufferableRequest> unsequencedRequests = new HashMap<>();

    String[] targets;
    ManagedChannel[] channels;
    DadkvsMainServiceGrpc.DadkvsMainServiceStub[] async_stubs;

    public DadkvsServerState(int kv_size, int port, int myself) {
        base_port = port;
        my_id = myself;
        configuration = 0; // Initial config.
        i_am_leader = false;
        debug_mode = 0;
        store_size = kv_size;
        sequence_number = 0;

        store = new KeyValueStore(kv_size);
        makeStubs(port);
        main_loop = new MainLoop(this);
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

        async_stubs = new DadkvsMainServiceGrpc.DadkvsMainServiceStub[n_servers];
        for (int i = 0; i < n_servers; i++) {
            async_stubs[i] = DadkvsMainServiceGrpc.newStub(channels[i]);
        }
    }

    public void terminateComms() {
        for (int i = 0; i < n_servers; i++) {
            channels[i].shutdownNow();
        }
    }

    public synchronized void incrementSequenceNumber() {
        sequence_number++;
    }

    public synchronized void setSequenceNumber(int sequence_number) {
        this.sequence_number = sequence_number;
    }

    public synchronized void crashServer() {
        System.out.println("Crashing the server...");

        terminateComms();
        if (main_loop_worker != null && main_loop_worker.isAlive()) {
            main_loop_worker.interrupt();
        }

		System.exit(1);
	}
}
