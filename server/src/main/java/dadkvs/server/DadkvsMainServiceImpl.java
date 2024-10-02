package dadkvs.server;

/* these imported classes are generated by the contract */
import dadkvs.DadkvsMain;
import dadkvs.DadkvsMainServiceGrpc;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

public class DadkvsMainServiceImpl extends DadkvsMainServiceGrpc.DadkvsMainServiceImplBase {

	DadkvsServerState server_state;
	int timestamp;

	public DadkvsMainServiceImpl(DadkvsServerState state) {
		this.server_state = state;
		this.timestamp = 0;
	}

	private void broadcastReadToReplicas(DadkvsMain.ReadRequest request, int sequenceNumber) {
		final int responses_needed = server_state.n_servers - 1;
		DadkvsMain.ReadRequest.Builder broadcastRequest = DadkvsMain.ReadRequest.newBuilder()
				.setKey(request.getKey())
				.setReqid(request.getReqid())
				.setSequenceNumber(sequenceNumber);

		ArrayList<DadkvsMain.ReadReply> broadcastResponses = new ArrayList<>();
		GenericResponseCollector<DadkvsMain.ReadReply> broadcastCollector = new GenericResponseCollector<>(
				broadcastResponses, server_state.n_servers);

		System.out.println("Leader broadcasting " + request.getReqid() + " with sequence nr " + sequenceNumber);
		for (int i = 0; i < server_state.n_servers; i++) {
			if (i == server_state.my_id)
				continue;
			CollectorStreamObserver<DadkvsMain.ReadReply> broadcastObserver = new CollectorStreamObserver<>(
					broadcastCollector);
			server_state.async_stubs[i].read(broadcastRequest.build(), broadcastObserver);
		}
		broadcastCollector.waitForTarget(responses_needed);
	}

	private boolean broadcastCommitToReplicas(DadkvsMain.CommitRequest request, int sequenceNumber) {
		final int responses_needed = server_state.n_servers - 1;
		boolean result = false;

		DadkvsMain.CommitRequest.Builder broadcastRequest = DadkvsMain.CommitRequest.newBuilder()
				.setReqid(request.getReqid())
				.setKey1(request.getKey1())
				.setVersion1(request.getVersion1())
				.setKey2(request.getKey2())
				.setVersion2(request.getVersion2())
				.setWritekey(request.getWritekey())
				.setWriteval(request.getWriteval())
				.setSequenceNumber(sequenceNumber);

		ArrayList<DadkvsMain.CommitReply> broadcastResponses = new ArrayList<DadkvsMain.CommitReply>();
		GenericResponseCollector<DadkvsMain.CommitReply> broadcastCollector = new GenericResponseCollector<DadkvsMain.CommitReply>(
				broadcastResponses, server_state.n_servers);

		System.out.println("Leader broadcasting " + request.getReqid() + " with sequence nr " + sequenceNumber);
		for (int i = 0; i < server_state.n_servers; i++) {
			if (i == server_state.my_id)
				continue;
			CollectorStreamObserver<DadkvsMain.CommitReply> broadcastObserver = new CollectorStreamObserver<DadkvsMain.CommitReply>(
					broadcastCollector);
			server_state.async_stubs[i].committx(broadcastRequest.build(), broadcastObserver);
		}
		broadcastCollector.waitForTarget(responses_needed);

		if (broadcastResponses.size() >= responses_needed) {
			Iterator<DadkvsMain.CommitReply> replyIterator = broadcastResponses.iterator();
			DadkvsMain.CommitReply reply = replyIterator.next();
			result = reply.getAck();
		}

		return result;
	}

	@Override
	public void read(DadkvsMain.ReadRequest request, StreamObserver<DadkvsMain.ReadReply> responseObserver) {
		// Freeze case
		if (this.server_state.debug_mode == 2) {
			System.out.println("Server is frozen, ignoring response.");
			return;

			// Slow mode one case
		} else if (this.server_state.debug_mode == 4) {
			Random random = new Random();
			// 100 + [0, 900] -> Range: [100, 1000] ms
			int delay = 100 + random.nextInt(901);

			try {
				Thread.sleep(delay);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			System.out.println("Slow-mode on, delaying: " + delay + " ms");
		}

		// Read
		int reqid = request.getReqid();
		int key = request.getKey();
		int req_seq_nr = request.getSequenceNumber();

		BufferableRequest br;
		if (server_state.i_am_leader) {
			br = new ReadBufferableRequest(
					reqid,
					responseObserver,
					server_state.sequence_number,
					key);

			broadcastReadToReplicas(request, server_state.sequence_number);
			server_state.workToDo.add(br);
		} else {
			if (req_seq_nr == -1) {
				br = server_state.unsequencedRequests.remove(reqid);
				if (br != null) { // server's request arrived before client's
					br.responseObserver = responseObserver;
					server_state.workToDo.add(br);
					System.out.println(
							"Non-leader received request " + reqid + " from client. Already had sequence number.");
					server_state.main_loop.wakeup();
					return;
				}

				// let's wait until we have a sequence number
				System.out.println("Non-leader received request " + reqid + " from client. Buffering...");
				br = new ReadBufferableRequest(reqid, responseObserver, key);
				server_state.unsequencedRequests.put(reqid, br);
				server_state.main_loop.wakeup();
				return;
			}

			// Coming from the leader.
			System.out.println("Non-leader received request " + reqid + " from leader");
			br = server_state.unsequencedRequests.get(reqid);
			if (br == null) {
				br = new ReadBufferableRequest(reqid, null, key);
				server_state.unsequencedRequests.put(reqid, br);
			} else {
				server_state.workToDo.add(br);
				server_state.unsequencedRequests.remove(reqid);
			}

			responseObserver.onNext(DadkvsMain.ReadReply.newBuilder()
					.setReqid(reqid)
					.setTimestamp(-1)
					.build());
			responseObserver.onCompleted();
		}

		server_state.main_loop.wakeup();
	}

	@Override
	public void committx(DadkvsMain.CommitRequest request, StreamObserver<DadkvsMain.CommitReply> responseObserver) {
		// for debug purposes
		int reqid = request.getReqid();
		int key1 = request.getKey1();
		int version1 = request.getVersion1();
		int key2 = request.getKey2();
		int version2 = request.getVersion2();
		int writekey = request.getWritekey();
		int writeval = request.getWriteval();
		int req_seq_nr = request.getSequenceNumber();

		// for debug purposes
		System.out.println("reqid " + reqid + " key1 " + key1 + " v1 " + version1 + " k2 " + key2 + " v2 " + version2
				+ " wk " + writekey + " writeval " + writeval + " seq " + req_seq_nr);

		// Freeze case
		if (this.server_state.debug_mode == 2) {
			System.out.println("Server is frozen, ignoring response.");
			return;

			// Slow mode one case
		} else if (this.server_state.debug_mode == 4) {
			Random random = new Random();
			// 100 + [0, 900] -> Range: [100, 1000] ms
			int delay = 100 + random.nextInt(901);

			try {
				Thread.sleep(delay);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			System.out.println("Slow-mode on, delaying: " + delay + " ms");
		}

		// Committx
		this.timestamp++;
		TransactionRecord txrecord = new TransactionRecord(
				key1,
				version1,
				key2,
				version2,
				writekey,
				writeval,
				this.timestamp);

		BufferableRequest br;
		if (server_state.i_am_leader) {
			System.out.println("Leader received request " + reqid + " from client...");

			br = new WriteBufferableRequest(
					reqid,
					responseObserver,
					server_state.sequence_number,
					txrecord);

			broadcastCommitToReplicas(request, server_state.sequence_number);
			server_state.workToDo.add(br);
		} else {
			if (req_seq_nr == -1) {
				// Coming from the client
				br = server_state.unsequencedRequests.remove(reqid);
				if (br != null) { // server's request arrived before client's
					br.responseObserver = responseObserver;
					server_state.workToDo.add(br);
					System.out.println(
							"Non-leader received request " + reqid + " from client. Already had sequence number.");
					server_state.main_loop.wakeup();
					return;
				}

				// let's wait until we have a sequence number
				System.out.println("Non-leader received request " + reqid + " from client. Buffering...");
				br = new WriteBufferableRequest(reqid, responseObserver, txrecord);
				server_state.unsequencedRequests.put(reqid, br);
				server_state.main_loop.wakeup();
				return;
			}

			// Coming from the leader.
			System.out.println("Non-leader received request " + reqid + " from leader");
			br = server_state.unsequencedRequests.get(reqid);
			if (br == null) {
				br = new WriteBufferableRequest(reqid, null, txrecord);
				server_state.unsequencedRequests.put(reqid, br);
			} else {
				server_state.workToDo.add(br);
				server_state.unsequencedRequests.remove(reqid);
			}

			responseObserver.onNext(DadkvsMain.CommitReply.newBuilder()
					.setAck(true)
					.setReqid(reqid)
					.build());
			responseObserver.onCompleted();
		}

		server_state.main_loop.wakeup();
	}
}
