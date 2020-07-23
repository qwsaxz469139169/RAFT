package ac.uk.ncl.gyc.raft.rpc;

/**
 * @author Yuchen Guo
 */
public interface RaftRpcClient {

    Response send(Request request);

}
