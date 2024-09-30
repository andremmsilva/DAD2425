package dadkvs.server;

import dadkvs.DadkvsMain;

public class MainLoop implements Runnable {
	DadkvsServerState server_state;

	private boolean has_work;

	public MainLoop(DadkvsServerState state) {
		this.server_state = state;
		this.has_work = false;
	}

	public void run() {
		while (true)
			this.doWork();
	}

	synchronized public void doWork() {
		System.out.println("Main loop do work start");
		this.has_work = false;
		while (this.has_work == false) {
			System.out.println("Main loop do work: waiting");
			try {
				wait();
			} catch (InterruptedException e) {
			}

			while (!server_state.workToDo.isEmpty()) {
				BufferableRequest br = server_state.workToDo.poll();
				boolean result = this.server_state.store.commit(br.record);

				// for debug purposes
				System.out.println("Result is ready for request with reqid " + br.reqId);

				DadkvsMain.CommitReply response = DadkvsMain.CommitReply.newBuilder()
						.setReqid(br.reqId).setAck(result).build();

				br.responseObserver.onNext(response);
				br.responseObserver.onCompleted();
			}
		}

		System.out.println("Main loop do work finish");
	}

	synchronized public void wakeup() {
		this.has_work = true;
		notify();
	}
}
