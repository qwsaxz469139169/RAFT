package ac.uk.ncl.gyc.raft.rpc;

/**
 * @author Yuchen Guo
 */
public interface RaftRpcServer {

    void start();

    void stop();

    Response handlerRequest(Request request);

}
