package dadkvs.server;

public class PaxosManager {

    public static int generateProposalNumber(int serverId, int counter) {
        return (counter * 10) + serverId;
    }
}
