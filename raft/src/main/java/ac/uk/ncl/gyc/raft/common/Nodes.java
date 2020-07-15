package ac.uk.ncl.gyc.raft.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * 节点集合. 去重.
 *
 * @author gyc
 */
public class Nodes implements Serializable {

    private List<PeerNode> nodesList = new ArrayList<>();

    private volatile PeerNode leader;

    /** final */
    private volatile PeerNode self;

    private Nodes() {
    }

    public static Nodes getInstance() {
        return getNodesInstance.INSTANCE;
    }

    private static class getNodesInstance {

        private static final Nodes INSTANCE = new Nodes();
    }

    public void setSelf(PeerNode peer) {
        self = peer;
    }

    public PeerNode getSelf() {
        return self;
    }

    public void addPeer(PeerNode peer) {
        nodesList.add(peer);
    }

    public void removePeer(PeerNode peer) {
        nodesList.remove(peer);
    }

    public List<PeerNode> getPeers() {
        return nodesList;
    }

    public List<PeerNode> getPeersWithOutSelf() {
        List<PeerNode> list2 = new ArrayList<>(nodesList);
        list2.remove(self);
        return list2;
    }


    public PeerNode getLeader() {
        return leader;
    }

    public void setLeader(PeerNode peer) {
        leader = peer;
    }

    @Override
    public String toString() {
        return "Nodes{" +
            "nodesList=" + nodesList +
            ", leader=" + leader +
            ", self=" + self +
            '}';
    }
}
