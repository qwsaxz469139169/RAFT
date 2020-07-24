package ac.uk.ncl.gyc.raft.impl;

import java.util.concurrent.locks.ReentrantLock;

import ac.uk.ncl.gyc.raft.Consensus;
import ac.uk.ncl.gyc.raft.common.PeerNode;
import ac.uk.ncl.gyc.raft.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ac.uk.ncl.gyc.raft.common.NodeStatus;
import io.netty.util.internal.StringUtil;

import static ac.uk.ncl.gyc.raft.impl.NodeImpl.received;
import static ac.uk.ncl.gyc.raft.impl.NodeImpl.startTime;

public class ConsensusImpl implements Consensus {


    private static final Logger LOGGER = LoggerFactory.getLogger(ConsensusImpl.class);


    public final NodeImpl node;

    public final ReentrantLock voteLock = new ReentrantLock();
    public final ReentrantLock appendLock = new ReentrantLock();
    public final ReentrantLock commitLock = new ReentrantLock();

    public ConsensusImpl(NodeImpl node) {
        this.node = node;
    }

    /**
     * 请求投票 RPC
     *
     * 接收者实现：
     *      如果term < currentTerm返回 false （5.2 节）
     *      如果 votedFor 为空或者就是 candidateId，并且候选人的日志至少和自己一样新，那么就投票给他（5.2 节，5.4 节）
     */
    @Override
    public ElectionTaskResponse requestVote(ElectionTaskRequest param) {
        System.out.println(param);
        try {
            ElectionTaskResponse.Builder builder = ElectionTaskResponse.newBuilder();
            if (!voteLock.tryLock()) {
                return builder.term(node.getCurrentTerm()).voteGranted(false).build();
            }

            // 对方任期没有自己新
            if (param.getTerm() < node.getCurrentTerm()) {
                System.out.println(1);
                return builder.term(node.getCurrentTerm()).voteGranted(false).build();
            }

            // (当前节点并没有投票 或者 已经投票过了且是对方节点) && 对方日志和自己一样新
            LOGGER.info("node {} current vote for [{}], param candidateId : {}", node.nodes.getSelf(), node.getVotedFor(), param.getCandidateId());
            LOGGER.info("node {} current term {}, peer term : {}", node.nodes.getSelf(), node.getCurrentTerm(), param.getTerm());

            if ((StringUtil.isNullOrEmpty(node.getVotedFor()) || node.getVotedFor().equals(param.getCandidateId()))) {

                if (node.getLogModule().getLast() != null) {
                    // 先比较term，term大的优先级大
                    if (node.getLogModule().getLast().getTerm() > param.getLastLogTerm()) {
                        return ElectionTaskResponse.fail();
                    }
                    // term >= 自己，再比较lastLogIndex
                    if (node.getLogModule().getLastIndex() > param.getLastLogIndex()) {
                        return ElectionTaskResponse.fail();
                    }
                }

                // 切换状态
                node.status = NodeStatus.FOLLOWER;
                // 更新
                node.nodes.setLeader(new PeerNode(param.getCandidateId()));
                node.setCurrentTerm(param.getTerm());
                node.setVotedFor(param.serverId);
                // 返回成功
                return builder.term(node.currentTerm).voteGranted(true).build();
            }

            return builder.term(node.currentTerm).voteGranted(false).build();

        } finally {
            voteLock.unlock();
        }
    }


    /**
     * 附加日志(多个日志,为了提高效率) RPC
     *
     * 接收者实现：
     *    如果 term < currentTerm 就返回 false （5.1 节）
     *    如果日志在 prevLogIndex 位置处的日志条目的任期号和 prevLogTerm 不匹配，则返回 false （5.3 节）
     *    如果已经存在的日志条目和新的产生冲突（索引值相同但是任期号不同），删除这一条和之后所有的 （5.3 节）
     *    附加任何在已有的日志中不存在的条目
     *    如果 leaderCommit > commitIndex，令 commitIndex 等于 leaderCommit 和 新日志条目索引值中较小的一个
     */
    @Override
    public LogTaskResponse appendEntries(LogTaskRequest param) {
        LogTaskResponse result = LogTaskResponse.fail();
        long startTime =  System.currentTimeMillis();
        try {
            appendLock.lock();

            result.setTerm(node.getCurrentTerm());
            // 不够格
            if (param.getTerm() < node.getCurrentTerm()) {
                return result;
            }

            node.preHeartBeatTime = System.currentTimeMillis();
            node.preElectionTime = System.currentTimeMillis();
            node.nodes.setLeader(new PeerNode(param.getLeaderId()));

            // 够格
            if (param.getTerm() >= node.getCurrentTerm()) {
                LOGGER.debug("node {} become FOLLOWER, currentTerm : {}, param Term : {}, param serverId",
                    node.nodes.getSelf(), node.currentTerm, param.getTerm(), param.getServerId());
                // 认怂
                node.status = NodeStatus.FOLLOWER;
            }
            // 使用对方的 term.
            node.setCurrentTerm(param.getTerm());

            //心跳
            if (param.getEntries() == null || param.getEntries().size() == 0) {
//                LOGGER.info("node {} append heartbeat success , Leader's term : {}, my term : {}",
//                    param.getLeaderId(), param.getTerm(), node.getCurrentTerm());
                return LogTaskResponse.newBuilder().term(node.getCurrentTerm()).success(true).build();
            }


            // 真实日志
            // 第一次
/*            if (node.getLogModule().getLastIndex() != 0 && param.getPrevLogIndex() != 0) {
                LogEntry logEntry;

                if ((logEntry = node.getLogModule().read(param.getPrevLogIndex())) != null) {
                    // 如果日志在 prevLogIndex 位置处的日志条目的任期号和 prevLogTerm 不匹配，则返回 false
                    // 需要减小 nextIndex 重试.
                    if (logEntry.getTerm() != param.getPreLogTerm()) {
                        return result;
                    }
                } else {
                    // index 不对, 需要递减 nextIndex 重试.
                    return result;
                }

            }

            // 如果已经存在的日志条目和新的产生冲突（索引值相同但是任期号不同），删除这一条和之后所有的
            LogEntry existLog = node.getLogModule().read(((param.getPrevLogIndex() + 1)));

            if (existLog != null && existLog.getTerm() != param.getEntries()[0].getTerm()) {
                // 删除这一条和之后所有的, 然后写入日志和状态机.
                node.getLogModule().removeOnStartIndex(param.getPrevLogIndex() + 1);
            } else if (existLog != null) {
                // 已经有日志了, 不能重复写入.
                result.setSuccess(true);
                return result;
            }*/


            // 写进日志并且应用到状态机
            for (LogEntry entry : param.getEntries()) {

                if(entry.isFirstIndex()){
                    String message = entry.getMessage();
                    if(node.received.get(message)==null){
                        node.received.put(message,1L);
                        node.startTime.put(message,startTime);
                    }

                }

                node.getLogModule().write(entry);
                node.stateMachine.apply(entry);

        }

            result.setSuccess(true);

            //如果 leaderCommit > commitIndex，令 commitIndex 等于 leaderCommit 和 新日志条目索引值中较小的一个
            if (param.getLeaderCommit() > node.getCommitIndex()) {
                int commitIndex = (int) Math.min(param.getLeaderCommit(), node.getLogModule().getLastIndex());
                node.setCommitIndex(commitIndex);
                node.setLastApplied(commitIndex);
            }

            result.setTerm(node.getCurrentTerm());

            node.status = NodeStatus.FOLLOWER;
            // TODO, 是否应当在成功回复之后, 才正式提交? 防止 leader "等待回复"过程中 挂掉.
            return result;
        } finally {
            appendLock.unlock();
        }
    }

    @Override
    public CommitResponse requestCommit(CommitRequest request) {

            String key = request.getMessage();

//            for(LogEntry logEntry :request.getLogEntries()){
//                System.out.println("333333333333333333333333333333");
//                System.out.println("The Message: "+logEntry.getMessage()+ "has been committed");
//            }

        System.out.println("The Message: "+key+ "has been committed");

            CommitResponse response = new CommitResponse();
               long latency = System.currentTimeMillis() - node.startTime.get(key);
        System.out.println("----------888888---------- "+System.currentTimeMillis());
        System.out.println("----------999999---------- "+node.startTime.get(key));
               System.out.println("----------111111----------- "+latency);
            response.setLatency(latency);
            response.setSuccess(true);

            node.received.remove(key);
            node.startTime.remove(key);
            return  response;

    }

    public NodeImpl getNode() {
        return node;
    }

    public ReentrantLock getVoteLock() {
        return voteLock;
    }

    public ReentrantLock getAppendLock() {
        return appendLock;
    }
}
