package ac.uk.ncl.gyc.raft;

import ac.uk.ncl.gyc.raft.common.NodesConfigration;
import ac.uk.ncl.gyc.raft.entity.*;
import ac.uk.ncl.gyc.raft.client.ClientResponse;
import ac.uk.ncl.gyc.raft.client.ClientRequest;

import java.util.List;

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
     * @param messages
     * @return
     */
    boolean handlerClientRequest( List<LogEntry> messages);

    /**
     * 转发给 leader 节点.
     * @param request
     * @return
     */
    ClientResponse redirect(ClientRequest request);

}
