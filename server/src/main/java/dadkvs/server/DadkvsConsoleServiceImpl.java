package dadkvs.server;

/* these imported classes are generated by the contract */
import dadkvs.DadkvsConsole;
import dadkvs.DadkvsConsoleServiceGrpc;

import io.grpc.stub.StreamObserver;

public class DadkvsConsoleServiceImpl extends DadkvsConsoleServiceGrpc.DadkvsConsoleServiceImplBase {

	DadkvsServerState server_state;

	public DadkvsConsoleServiceImpl(DadkvsServerState state) {
		this.server_state = state;
	}

	@Override
	public void setleader(DadkvsConsole.SetLeaderRequest request,
			StreamObserver<DadkvsConsole.SetLeaderReply> responseObserver) {
		// for debug purposes
		System.out.println(request);

		boolean response_value = true;
		this.server_state.i_am_leader = request.getIsleader();

		// for debug purposes
		System.out.println("I am the leader = " + this.server_state.i_am_leader);

		this.server_state.main_loop.wakeup();

		DadkvsConsole.SetLeaderReply response = DadkvsConsole.SetLeaderReply.newBuilder()
				.setIsleaderack(response_value).build();

		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	@Override
	public void setdebug(DadkvsConsole.SetDebugRequest request,
			StreamObserver<DadkvsConsole.SetDebugReply> responseObserver) {
		// for debug purposes
		System.out.println(request);

		boolean response_value = true;
		this.server_state.debug_mode = request.getMode();

		// If it's a crash debugmode
		if(this.server_state.debug_mode == 1) {
			server_state.crashServer();
		}

		// If it's to do an unfreeze or an slow-mode-off it will just go to normal (0)
		else if(this.server_state.debug_mode == 3 || this.server_state.debug_mode == 5){
			this.server_state.debug_mode = 0;
			this.server_state.main_loop.wakeup();
		} else {
			this.server_state.main_loop.wakeup();
		}

		// for debug purposes
		System.out.println("Setting debug mode to = " + this.server_state.debug_mode);

		DadkvsConsole.SetDebugReply response = DadkvsConsole.SetDebugReply.newBuilder()
				.setAck(response_value).build();

		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

    @Override
    public void reconfigure(DadkvsConsole.ReconfigureRequest request,
                            StreamObserver<DadkvsConsole.ReconfigureReply> responseObserver) {
        // for debug purposes
        System.out.println(request);

        boolean response_value = true;
        int new_configuration = request.getConfiguration();

        if (new_configuration == this.server_state.configuration + 1) {
            this.server_state.configuration = new_configuration;
            System.out.println("Reconfiguring to configuration " + new_configuration);
        } else {
            // If the configuration is not the next in sequence, reject the request
            System.out.println("Invalid configuration request: " + new_configuration);
            response_value = false;
        }

        // Wake up the main loop after reconfiguration
        this.server_state.main_loop.wakeup();

        // Send the response back to the client
        DadkvsConsole.ReconfigureReply response = DadkvsConsole.ReconfigureReply.newBuilder()
                .setAck(response_value).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
