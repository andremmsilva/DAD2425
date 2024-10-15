package dadkvs.server;

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

				int lastIdx = this.server_state.lastExecutedIdx.get();
				Operation o = this.server_state.operationLog.get(lastIdx);
				if (!o.canExecute) {
					break;
				}

				GenericRequest<?> req = this.server_state.pendingRequests.get(o.reqId);
				if (req == null) {
					break; // Don't execute yet, wait for client to send the request.
				}

				req.process(this.server_state);
				this.server_state.lastExecutedIdx.set(lastIdx + 1);
			} catch (InterruptedException e) {
			}
		}
		System.out.println("Main loop do work finish");
	}

	synchronized public void wakeup() {
		this.has_work = true;
		notify();
	}
}
