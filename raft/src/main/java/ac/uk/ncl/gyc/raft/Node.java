package ac.uk.ncl.gyc.raft;

import ac.uk.ncl.gyc.raft.common.NodesConfigration;
import ac.uk.ncl.gyc.raft.entity.ElectionTaskRequest;
import ac.uk.ncl.gyc.raft.entity.LogTaskRequest;
import ac.uk.ncl.gyc.raft.entity.LogTaskResponse;
import ac.uk.ncl.gyc.raft.entity.ElectionTaskResponse;
import ac.uk.ncl.gyc.raft.client.ClientResponse;
import ac.uk.ncl.gyc.raft.client.ClientRequest;

/**
 *
 * @author Yuchen Guo
 */
public interface Node<T> extends LifeCycle{

    /**
     * 设置配置文件.
     *
     * @param config
     */
    void setConfig(NodesConfigration config);

    /**
     * 处理请求投票 RPC.
     *
     * @param param
     * @return
     */
    ElectionTaskResponse handlerRequestVote(ElectionTaskRequest param);

    /**
     * 处理附加日志请求.
     *
     * @param param
     * @return
     */
    LogTaskResponse handlerAppendEntries(LogTaskRequest param);

    /**
     * 处理客户端请求.
     *
     * @param request
     * @return
     */
    ClientResponse handlerClientRequest(ClientRequest request);

    /**
     * 转发给 leader 节点.
     * @param request
     * @return
     */
    ClientResponse redirect(ClientRequest request);

}
