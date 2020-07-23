package ac.uk.ncl.gyc.raft.membership.changes;

import ac.uk.ncl.gyc.raft.common.PeerNode;

public interface ClusterMembershipChanges {

    /**
     * 添加节点.
     *
     * @param newNode
     * @return
     */
    Result addNode(PeerNode newNode);

    /**
     * 删除节点.
     *
     * @param oldNode
     * @return
     */
    Result removeNode(PeerNode oldNode);
}

