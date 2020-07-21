package ac.uk.ncl.gyc.raft.rpc;

import ac.uk.ncl.gyc.raft.common.PeerNode;
import ac.uk.ncl.gyc.raft.entity.CommitRequest;
import ac.uk.ncl.gyc.raft.impl.NodeImpl;
import ac.uk.ncl.gyc.raft.entity.ElectionTaskRequest;
import com.alipay.remoting.BizContext;

import ac.uk.ncl.gyc.raft.entity.LogTaskRequest;
import ac.uk.ncl.gyc.raft.membership.changes.ClusterMembershipChanges;
import com.alipay.remoting.rpc.RpcServer;
import ac.uk.ncl.gyc.raft.client.ClientRequest;


@SuppressWarnings("unchecked")
public class RaftRpcServerImpl implements RaftRpcServer {

    private volatile boolean flag;

    private NodeImpl node;

    private RpcServer rpcServer;

    public RaftRpcServerImpl(int port, NodeImpl node) {

        if (flag) {
            return;
        }
        synchronized (this) {
            if (flag) {
                return;
            }

            rpcServer = new RpcServer(port, false, false);

            rpcServer.registerUserProcessor(new RaftUserProcessor<Request>() {

                @Override
                public Object handleRequest(BizContext bizCtx, Request request) throws Exception {
                    return handlerRequest(request);
                }
            });

            this.node = node;
            flag = true;
        }

    }

    @Override
    public void start() {
        rpcServer.start();
    }

    @Override
    public void stop() {
        rpcServer.stop();
    }

    @Override
    public Response handlerRequest(Request request) {

        long receiveTime = System.currentTimeMillis();
        if (request.getCmd() == Request.REQ_VOTE) {
            return new Response(node.handlerRequestVote((ElectionTaskRequest) request.getObj()));
        } else if (request.getCmd() == Request.REQ_LOG) {
            return new Response(node.handlerAppendEntries((LogTaskRequest) request.getObj()));
        } else if (request.getCmd() == Request.REQ_CLIENT) {
            return new Response(node.handlerClientRequest((ClientRequest) request.getObj(),receiveTime));
        } else if (request.getCmd() == Request.CHANGE_CONFIG_REMOVE) {
            return new Response(((ClusterMembershipChanges) node).removeNode((PeerNode) request.getObj()));
        } else if (request.getCmd() == Request.CHANGE_CONFIG_ADD) {
            return new Response(((ClusterMembershipChanges) node).addNode((PeerNode) request.getObj()));
        }else if(request.getCmd() == Request.REQ_COMMIT){
            return new Response(node.handlerCommitRequest((CommitRequest) request.getObj()));
        }
        return null;
    }


}
