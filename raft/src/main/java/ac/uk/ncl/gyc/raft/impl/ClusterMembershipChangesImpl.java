package ac.uk.ncl.gyc.raft.impl;

import ac.uk.ncl.gyc.raft.common.PeerNode;
import ac.uk.ncl.gyc.raft.entity.LogEntry;
import ac.uk.ncl.gyc.raft.rpc.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ac.uk.ncl.gyc.raft.common.NodeStatus;
import ac.uk.ncl.gyc.raft.membership.changes.ClusterMembershipChanges;
import ac.uk.ncl.gyc.raft.membership.changes.Result;
import ac.uk.ncl.gyc.raft.rpc.Response;

public class ClusterMembershipChangesImpl implements ClusterMembershipChanges {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterMembershipChangesImpl.class);


    private final NodeImpl node;

    public ClusterMembershipChangesImpl(NodeImpl node) {
        this.node = node;
    }

    /** 必须是同步的,一次只能添加一个节点
     * @param newNode*/
    @Override
    public synchronized Result addNode(PeerNode newNode) {
        // 已经存在
        if (node.nodes.getPeersWithOutSelf().contains(newNode)) {
            return new Result();
        }

        node.nodes.getPeersWithOutSelf().add(newNode);

        if (node.status == NodeStatus.LEADER) {
            node.nextIndexs.put(newNode, 0L);
            node.matchIndexs.put(newNode, 0L);

            for (long i = 0; i < node.logModule.getLastIndex(); i++) {
                LogEntry e = node.logModule.read(i);
                if (e != null) {
                    node.replication(newNode, e);
                }
            }

            for (PeerNode item : node.nodes.getPeersWithOutSelf()) {
                // TODO 同步到其他节点.
                Request request = Request.newBuilder()
                    .cmd(Request.CHANGE_CONFIG_ADD)
                    .url(newNode.getAdress())
                    .obj(newNode)
                    .build();

                Response response = node.raftRpcClient.send(request);
                Result result = (Result) response.getResult();
                if (result != null && result.getStatus() == Result.Status.SUCCESS.getCode()) {
                    LOGGER.info("replication config success, peer : {}, newServer : {}", newNode, newNode);
                } else {
                    LOGGER.warn("replication config fail, peer : {}, newServer : {}", newNode, newNode);
                }
            }

        }

        return new Result();
    }


    /** 必须是同步的,一次只能删除一个节点
     * @param oldNode*/
    @Override
    public synchronized Result removeNode(PeerNode oldNode) {
        node.nodes.getPeersWithOutSelf().remove(oldNode);
        node.nextIndexs.remove(oldNode);
        node.matchIndexs.remove(oldNode);

        return new Result();
    }
}
