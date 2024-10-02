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
			try {
				wait();
			} catch (InterruptedException e) {
			}

			while (!server_state.workToDo.isEmpty()) {
				BufferableRequest<?> br = server_state.workToDo.peek();
				if (br.sequenceNumber <= server_state.sequence_number) {
					server_state.workToDo.poll();

					System.out.println("Main loop doing request " + br.reqId);

					br.process(server_state);
					server_state.incrementSequenceNumber();
				} else {
					break;
				}
			}
		}

		System.out.println("Main loop do work finish");
	}

	synchronized public void wakeup() {
		this.has_work = true;
		notify();
	}
}
