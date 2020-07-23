package ac.uk.ncl.gyc.raft.impl;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import ac.uk.ncl.gyc.raft.LogModule;
import ac.uk.ncl.gyc.raft.client.Message;
import ac.uk.ncl.gyc.raft.common.PeerNode;
import ac.uk.ncl.gyc.raft.entity.*;
import ac.uk.ncl.gyc.raft.rpc.RaftRpcClient;
import ac.uk.ncl.gyc.raft.rpc.RaftRpcClientImpl;
import ac.uk.ncl.gyc.raft.rpc.Request;
import ac.uk.ncl.gyc.raft.common.Nodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ac.uk.ncl.gyc.raft.Consensus;
import ac.uk.ncl.gyc.raft.LifeCycle;
import ac.uk.ncl.gyc.raft.Node;
import ac.uk.ncl.gyc.raft.StateMachine;
import ac.uk.ncl.gyc.raft.common.NodesConfigration;
import ac.uk.ncl.gyc.raft.common.NodeStatus;
import ac.uk.ncl.gyc.raft.current.RaftThreadPool;
import ac.uk.ncl.gyc.raft.exception.RaftRemotingException;
import ac.uk.ncl.gyc.raft.membership.changes.ClusterMembershipChanges;
import ac.uk.ncl.gyc.raft.membership.changes.Result;
import ac.uk.ncl.gyc.raft.rpc.RaftRpcServerImpl;
import ac.uk.ncl.gyc.raft.rpc.Response;
import ac.uk.ncl.gyc.raft.rpc.RaftRpcServer;
import ac.uk.ncl.gyc.raft.util.LongConvert;
import lombok.Getter;
import lombok.Setter;
import ac.uk.ncl.gyc.raft.client.ClientResponse;
import ac.uk.ncl.gyc.raft.client.ClientRequest;

import static ac.uk.ncl.gyc.raft.common.NodeStatus.LEADER;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class NodeImpl<T> implements Node<T>, LifeCycle, ClusterMembershipChanges {

    private static final Logger LOGGER = LoggerFactory.getLogger(NodeImpl.class);

    /**
     * 选举时间间隔基数
     */
    public volatile long electionTime = 15 * 1000;
    /**
     * 上一次选举时间
     */
    public volatile long preElectionTime = 0;

    /**
     * 上次一心跳时间戳
     */
    public volatile long preHeartBeatTime = 0;
    /**
     * 心跳间隔基数
     */
    public final long heartBeatTick = 5 * 1000;


    private HeartBeatTask heartBeatTask = new HeartBeatTask();
    private ElectionTask electionTask = new ElectionTask();
    private ReplicationFailQueueConsumer replicationFailQueueConsumer = new ReplicationFailQueueConsumer();

    private LinkedBlockingQueue<ReplicationFailModel> replicationFailQueue = new LinkedBlockingQueue<>(2048);


    /**
     * 节点当前状态
     *
     * @see NodeStatus
     */
    public volatile int status = NodeStatus.FOLLOWER;

    public Nodes nodes;



    /* ============ 所有服务器上持久存在的 ============= */

    /**
     * 服务器最后一次知道的任期号（初始化为 0，持续递增）
     */
    volatile long currentTerm = 0;
    /**
     * 在当前获得选票的候选人的 Id
     */
    volatile String votedForId;
    /**
     * 日志条目集；每一个条目包含一个用户状态机执行的指令，和收到时的任期号
     */
    LogModule logModule;



    /* ============ 所有服务器上经常变的 ============= */

    /**
     * 已知的最大的已经被提交的日志条目的索引值
     */
    volatile long commitIndex;

    /**
     * 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增)
     */
    volatile long lastApplied = 0;

    /* ========== 在领导人里经常改变的(选举后重新初始化) ================== */

    /**
     * 对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一）
     */
    Map<PeerNode, Long> nextIndexs;

    /**
     * 对于每一个服务器，已经复制给他的日志的最高索引值
     */
    Map<PeerNode, Long> matchIndexs;



    /* ============================== */

    public volatile boolean started;

    public NodesConfigration config;

    public static RaftRpcServer RPC_SERVER;

    public RaftRpcClient raftRpcClient = new RaftRpcClientImpl();

    public StateMachine stateMachine;

    public static Map<String, Long> received = new ConcurrentHashMap();

    public static Map<String, Long> startTime = new ConcurrentHashMap();

    /* ============================== */

    /**
     * 一致性模块实现
     */
    Consensus consensus;

    ClusterMembershipChanges cluster;


    /* ============================== */

    public static Map<String, CopyOnWriteArrayList<Long>> latencyMap = new ConcurrentHashMap();

    public static Map<String, Boolean> isRedirect = new ConcurrentHashMap();

    public static Map<String, Long> leader_latencyMap = new ConcurrentHashMap();

    public static CopyOnWriteArrayList<String> ACKS = new CopyOnWriteArrayList<>();

    public static Map<String, Integer> LEADER_ACKS = new ConcurrentHashMap();


    private NodeImpl() {
    }

    public static NodeImpl getInstance() {
        return DefaultNodeLazyHolder.INSTANCE;
    }


    private static class DefaultNodeLazyHolder {

        private static final NodeImpl INSTANCE = new NodeImpl();
    }

    @Override
    public void init() throws Throwable {
        if (started) {
            return;
        }


        synchronized (this) {
            if (started) {
                return;
            }
            RPC_SERVER.start();

            consensus = new ConsensusImpl(this);
            cluster = new ClusterMembershipChangesImpl(this);

            RaftThreadPool.scheduleWithFixedDelay(heartBeatTask, 500);
            RaftThreadPool.scheduleAtFixedRate(electionTask, 6000, 500);
            RaftThreadPool.execute(replicationFailQueueConsumer);

            LogEntry logEntry = logModule.getLast();
            if (logEntry != null) {
                currentTerm = logEntry.getTerm();
            }

            started = true;

            LOGGER.info("start success, Current server id : {} ", nodes.getSelf());
        }
    }

    @Override
    public void setConfig(NodesConfigration config) {
        this.config = config;
        stateMachine = StateMachineImpl.getInstance();
        logModule = LogModuleImpl.getInstance();

        nodes = Nodes.getInstance();

        for (String s : config.getPeerAddrs()) {
            PeerNode peer = new PeerNode(s);
            nodes.addPeer(peer);

            if (s.equals("localhost:" + config.getSelfPort())) {
                System.out.println("设置自身IP：" + s);
                nodes.setSelf(peer);
            }
        }


        RPC_SERVER = new RaftRpcServerImpl(config.selfPort, this);
    }


    @Override
    public ElectionTaskResponse handlerRequestVote(ElectionTaskRequest param) {
        LOGGER.warn("handlerRequestVote will be invoke, param info : {}", param);
        return consensus.requestVote(param);
    }


    public LogTaskResponse handlerAppendEntries(LogTaskRequest param) {
        if (param.getEntries() != null) {
            LOGGER.warn("node receive node {} append entry, entry content = {}", param.getLeaderId(), param.getEntries());
        }
        return consensus.appendEntries(param);
    }

    public CommitResponse handlerCommitRequest(CommitRequest request) {
        LOGGER.warn("handlerCommitRequest will be invoke, param info : {}", request);
        return consensus.requestCommit(request);
    }


    @SuppressWarnings("unchecked")
    @Override
    public ClientResponse redirect(ClientRequest request) {
        Request<ClientRequest> r = Request.newBuilder().
                obj(request).url(nodes.getLeader().getAdress()).cmd(Request.REQ_CLIENT).build();
        Response response = raftRpcClient.send(r);
        return (ClientResponse) response.getResult();
    }

    /**
     * 客户端的每一个请求都包含一条被复制状态机执行的指令。
     * 领导人把这条指令作为一条新的日志条目附加到日志中去，然后并行的发起附加条目 RPCs 给其他的服务器，让他们复制这条日志条目。
     * 当这条日志条目被安全的复制（下面会介绍），领导人会应用这条日志条目到它的状态机中然后把执行的结果返回给客户端。
     * 如果跟随者崩溃或者运行缓慢，再或者网络丢包，
     * 领导人会不断的重复尝试附加日志条目 RPCs （尽管已经回复了客户端）直到所有的跟随者都最终存储了所有的日志条目。
     *
     * @param request
     * @return
     */
    @Override
    public synchronized ClientResponse handlerClientRequest(ClientRequest request, long receiveTime) {

        LOGGER.warn("handlerClientRequest handler {} operation,  and key : [{}], value : [{}]",
                ClientRequest.Type.value(request.getType()), request.getKey(), request.getValue());

        if (status != LEADER) {
            LOGGER.warn("Current node is not Leader , redirect to leader node, leader addr : {}, my addr : {}",
                    nodes.getLeader(), nodes.getSelf().getAdress());

            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("cur redirect contain ack count: " + ACKS.size());
            if (ACKS.size() > 0) {
                List<String> ac = new ArrayList<>();
                for (String a : ACKS) {
                    ac.add(a);
                    ACKS.remove(a);
                }
                request.setAcks(ac);
            }

            received.put(request.getKey(), 1L);
            startTime.put(request.getKey(), receiveTime);

            request.setRedirect(true);
            return redirect(request);
        }

        //record extra messages
        if (status == LEADER) {
            if (request.isRedirect()) {
                leader_latencyMap.put(request.getKey(), receiveTime - 2);
                isRedirect.put(request.getKey(), true);
            } else {
                leader_latencyMap.put(request.getKey(), receiveTime);
                isRedirect.put(request.getKey(), false);
            }

            latencyMap.put(request.getKey(), new CopyOnWriteArrayList<>());
        }


        if (request.getType() == ClientRequest.GET) {
            LogEntry logEntry = stateMachine.get(request.getKey());
            if (logEntry != null) {
                return new ClientResponse(logEntry.getCommand());
            }
            return new ClientResponse(null);
        }
        try {
            Thread.sleep(2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        List<String> commitList = new ArrayList<>();
        if (request.getAcks() != null) {
            System.out.println("cur receive ack count: " + request.getAcks().size());
            for (String ack : request.getAcks()) {
                if (LEADER_ACKS.get(ack) != null) {
                    int ack_cout = LEADER_ACKS.get(ack);
                    LEADER_ACKS.put(ack, ack_cout + 1);
                    commitList.add(ack);
                    LEADER_ACKS.remove(ack);
                } else {
                    LEADER_ACKS.put(ack, 1);
                }
            }
        } else {
            System.out.println("cur receive ack count: 0 ");
        }
        System.out.println("cur pre commit message count: " + commitList.size());


        LogEntry logEntry = new LogEntry();
        logEntry.setTerm(currentTerm);
        logEntry.setMessage(request.getKey());
        logEntry.setStartTime(receiveTime);
        logEntry.setCommand(Command.newBuilder().
                key(request.getKey()).
                value(request.getValue()).
                build());

        logEntry.setCommitList(commitList);

        // 预提交到本地日志, TODO 预提交
        logModule.write(logEntry);


        final AtomicInteger success = new AtomicInteger(0);

        List<Future<Boolean>> futureList = new CopyOnWriteArrayList<>();
        int count = 0;
        //  复制到其他机器
        for (PeerNode peer : nodes.getPeersWithOutSelf()) {
            // TODO check self and RaftThreadPool
            count++;
            // 并行发起 RPC 复制.
            futureList.add(replication(peer, logEntry));
        }

        CountDownLatch latch = new CountDownLatch(futureList.size());
        List<Boolean> resultList = new CopyOnWriteArrayList<>();

        getRPCAppendResult(futureList, latch, resultList);

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (Boolean aBoolean : resultList) {
            if (aBoolean) {
                success.incrementAndGet();
            }
        }
        // 如果存在一个满足N > commitIndex的 N，并且大多数的matchIndex[i] ≥ N成立，
        // 并且log[N].term == currentTerm成立，那么令 commitIndex 等于这个 N （5.3 和 5.4 节）
        List<Long> matchIndexList = new ArrayList<>(matchIndexs.values());
        // 小于 2, 没有意义
        int median = 0;
        if (matchIndexList.size() >= 2) {
            Collections.sort(matchIndexList);
            median = matchIndexList.size() / 2;
        }
        Long N = matchIndexList.get(median);
        if (N > commitIndex) {
            LogEntry entry = logModule.read(N);
            if (entry != null && entry.getTerm() == currentTerm) {
                commitIndex = N;
            }
        }
        if (success.get() >= count) {
            commitIndex = logEntry.getIndex();
            //  应用到状态机
            getStateMachine().apply(logEntry);
            lastApplied = commitIndex;
            List<Message> mess = new ArrayList<>();
            System.out.println("response client, committed message count: " + commitList.size());
            if (commitList.size() > 0) {
                for (String M_NAME : commitList) {
                    System.out.println(" committed message: " + M_NAME);
                    long followerLatency2 = 0;
                    long leaderLatency2 = System.currentTimeMillis() - leader_latencyMap.get(M_NAME);
                    System.out.println(" leader_latencyMap.get(M_NAME): " + leader_latencyMap.get(M_NAME));
                    System.out.println(" leaderLatency2: " + leaderLatency2);
                    for (long a : latencyMap.get(M_NAME)) {
                        System.out.println(" a: " + a);
                        followerLatency2 = followerLatency2 + a;
                    }
                    followerLatency2 = followerLatency2 / latencyMap.get(M_NAME).size();
                    System.out.println(" followerLatency2: " + followerLatency2);
                    Message m = new Message(M_NAME, leaderLatency2, followerLatency2);
                    mess.add(m);
                    latencyMap.remove(M_NAME);
                    leader_latencyMap.remove(M_NAME);
                }
            }
            ClientResponse clientResponse = new ClientResponse();
            clientResponse.setMessages(mess);
            clientResponse.setResult("ok");
            return clientResponse;

        } else {
            return ClientResponse.ok();
        }


    }

    private boolean ReqCommit(List<String> commitList) {
        final AtomicInteger success = new AtomicInteger(0);

        List<Future<Boolean>> futureList = new CopyOnWriteArrayList<>();

        int count = 0;
        //  复制到其他机器
        for (PeerNode peer : nodes.getPeersWithOutSelf()) {
            // TODO check self and RaftThreadPool
            count++;

            futureList.add(Commit(peer, commitList));
        }

        CountDownLatch latch = new CountDownLatch(futureList.size());
        List<Boolean> resultList = new CopyOnWriteArrayList<>();
        getRPCAppendResult(futureList, latch, resultList);

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (Boolean aBoolean : resultList) {
            if (aBoolean) {
                success.incrementAndGet();
            }
        }

        if (success.get() >= count) {
            return true;
        } else {
            return false;
        }
    }

    public Future<Boolean> Commit(PeerNode peer, List<String> commitList) {

        return RaftThreadPool.submit(new Callable() {
            @Override
            public Boolean call() throws Exception {

                long start = System.currentTimeMillis(), end = start;

                // 20 秒重试时间
                while (end - start < 20 * 1000L) {
                    String key = "";

                    CommitRequest commitRequest = new CommitRequest();
                    commitRequest.setTerm(currentTerm);
                    commitRequest.setServerId(peer.getAdress());
                    commitRequest.setMessages(commitList);

                    Request request = Request.newBuilder()
                            .cmd(Request.REQ_COMMIT)
                            .obj(commitRequest)
                            .url(peer.getAdress())
                            .build();


                    try {
                        Response response = getRaftRpcClient().send(request);
                        if (response == null) {
                            continue;
                        }
                        CommitResponse result = (CommitResponse) response.getResult();

                        if (result != null && result.isSuccess()) {
                            LOGGER.info("commit success , follower=[{}], entry=[{}]", peer, key);

                            Map<String, Long> lMap = result.getLatency();

                            for (String mName : lMap.keySet()) {
                                CopyOnWriteArrayList<Long> latencyList = latencyMap.get(mName);
                                latencyList.add(lMap.get(mName));
                                latencyMap.put(mName, latencyList);
                            }


                            return true;
                        }

                        end = System.currentTimeMillis();

                    } catch (Exception e) {
                        continue;
                    }
                }
                // 超时了,没办法了
                return false;
            }
        });

    }

    private void getRPCAppendResult(List<Future<Boolean>> futureList, CountDownLatch latch, List<Boolean> resultList) {
        for (Future<Boolean> future : futureList) {
            RaftThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        resultList.add(future.get());
                    } catch (CancellationException | ExecutionException | InterruptedException e) {
                        e.printStackTrace();
                        resultList.add(false);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
    }


    /**
     * 复制到其他机器
     */
    public Future<Boolean> replication(PeerNode peer, LogEntry entry) {

        return RaftThreadPool.submit(new Callable() {
            @Override
            public Boolean call() throws Exception {

                long start = System.currentTimeMillis(), end = start;

                LogTaskRequest logTaskRequest = new LogTaskRequest();
                logTaskRequest.setTerm(currentTerm);
                logTaskRequest.setServerId(peer.getAdress());
                logTaskRequest.setLeaderId(nodes.getSelf().getAdress());

                logTaskRequest.setLeaderCommit(commitIndex);

                // 以我这边为准, 这个行为通常是成为 leader 后,首次进行 RPC 才有意义.
                Long nextIndex = nextIndexs.get(peer);
                LinkedList<LogEntry> logEntries = new LinkedList<>();
                if (entry.getIndex() >= nextIndex) {
                    for (long i = nextIndex; i <= entry.getIndex(); i++) {
                        LogEntry l = logModule.read(i);
                        if (l != null) {
                            logEntries.add(l);
                        }
                    }
                } else {
                    logEntries.add(entry);
                }
                // 最小的那个日志.
                LogEntry preLog = getPreLog(logEntries.getFirst());
                logTaskRequest.setPreLogTerm(preLog.getTerm());
                logTaskRequest.setPrevLogIndex(preLog.getIndex());

                logTaskRequest.setEntries(logEntries.toArray(new LogEntry[0]));

                Request request = Request.newBuilder()
                        .cmd(Request.REQ_LOG)
                        .obj(logTaskRequest)
                        .url(peer.getAdress())
                        .build();


                try {
                    Response response = getRaftRpcClient().send(request);
                    if (response == null) {
                        return false;
                    }
                    LogTaskResponse result = (LogTaskResponse) response.getResult();
                    if (result != null && result.isSuccess()) {
                        LOGGER.info("append follower entry success , follower=[{}], entry=[{}]", peer, logTaskRequest.getEntries());
                        // update 这两个追踪值
                        nextIndexs.put(peer, entry.getIndex() + 1);
                        matchIndexs.put(peer, entry.getIndex());

                        Map<String, Long> lMap = result.getCommittedList();

                        for (Map.Entry<String, Long> entry1 : lMap.entrySet()) {
                            String mName = entry1.getKey();
                            long latency = entry1.getValue();
                            CopyOnWriteArrayList<Long> latencyList = latencyMap.get(mName);
                            latencyList.add(latency);
                            latencyMap.put(mName, latencyList);
                        }

                        return true;
                    } else if (result != null) {
                        // 对方比我大
                        if (result.getTerm() > currentTerm) {
                            LOGGER.warn("follower [{}] term [{}] than more self, and my term = [{}], so, I will become follower",
                                    peer, result.getTerm(), currentTerm);
                            currentTerm = result.getTerm();
                            // 认怂, 变成跟随者
                            status = NodeStatus.FOLLOWER;
                            return false;
                        } // 没我大, 却失败了,说明 index 不对.或者 term 不对.
                        else {
                            // 递减
                            if (nextIndex == 0) {
                                nextIndex = 1L;
                            }
                            nextIndexs.put(peer, nextIndex - 1);
                            LOGGER.warn("follower {} nextIndex not match, will reduce nextIndex and retry RPC append, nextIndex : [{}]", peer.getAdress(),
                                    nextIndex);
                            // 重来, 直到成功.
                        }
                    }

                    end = System.currentTimeMillis();

                } catch (Exception e) {
                    LOGGER.warn(e.getMessage(), e);
                    // TODO 到底要不要放队列重试?
//                        ReplicationFailModel model =  ReplicationFailModel.newBuilder()
//                            .callable(this)
//                            .logEntry(entry)
//                            .peer(peer)
//                            .offerTime(System.currentTimeMillis())
//                            .build();
//                        replicationFailQueue.offer(model);
                    return false;
                }

                // 超时了,没办法了
                return false;
            }
        });

    }

    private LogEntry getPreLog(LogEntry logEntry) {
        LogEntry entry = logModule.read(logEntry.getIndex() - 1);

        if (entry == null) {
            LOGGER.warn("get perLog is null , parameter logEntry : {}", logEntry);
            entry = LogEntry.newBuilder().index(0L).term(0).command(null).build();
        }
        return entry;
    }


    class ReplicationFailQueueConsumer implements Runnable {

        /**
         * 一分钟
         */
        long intervalTime = 1000 * 60;

        @Override
        public void run() {
            for (; ; ) {

                try {
                    ReplicationFailModel model = replicationFailQueue.take();
                    if (status != LEADER) {
                        // 应该清空?
                        replicationFailQueue.clear();
                        continue;
                    }
                    LOGGER.warn("replication Fail Queue Consumer take a task, will be retry replication, content detail : [{}]", model.logEntry);
                    long offerTime = model.offerTime;
                    if (System.currentTimeMillis() - offerTime > intervalTime) {
                        LOGGER.warn("replication Fail event Queue maybe full or handler slow");
                    }

                    Callable callable = model.callable;
                    Future<Boolean> future = RaftThreadPool.submit(callable);
                    Boolean r = future.get(3000, MILLISECONDS);
                    // 重试成功.
                    if (r) {
                        // 可能有资格应用到状态机.
                        tryApplyStateMachine(model);
                    }

                } catch (InterruptedException e) {
                    // ignore
                } catch (ExecutionException | TimeoutException e) {
                    LOGGER.warn(e.getMessage());
                }
            }
        }
    }

    private void tryApplyStateMachine(ReplicationFailModel model) {

        String success = stateMachine.getString(model.successKey);
        stateMachine.setString(model.successKey, String.valueOf(Integer.valueOf(success) + 1));

        String count = stateMachine.getString(model.countKey);

        if (Integer.valueOf(success) >= Integer.valueOf(count) / 2) {
            stateMachine.apply(model.logEntry);
            stateMachine.delString(model.countKey, model.successKey);
        }
    }


    @Override
    public void destroy() throws Throwable {
        RPC_SERVER.stop();
    }


    /**
     * 1. 在转变成候选人后就立即开始选举过程
     * 自增当前的任期号（currentTerm）
     * 给自己投票
     * 重置选举超时计时器
     * 发送请求投票的 RPC 给其他所有服务器
     * 2. 如果接收到大多数服务器的选票，那么就变成领导人
     * 3. 如果接收到来自新的领导人的附加日志 RPC，转变成跟随者
     * 4. 如果选举过程超时，再次发起一轮选举
     */
    class ElectionTask implements Runnable {

        @Override
        public void run() {

            if (status == LEADER) {
                return;
            }

            long current = System.currentTimeMillis();

            // 基于 RAFT 的随机时间,解决冲突.
            electionTime = electionTime + ThreadLocalRandom.current().nextInt(50);

            if (current - preElectionTime < electionTime) {
                return;
            }

            status = NodeStatus.CANDIDATE;

            LOGGER.error("node {} will become CANDIDATE and start election leader, current term : [{}], LastEntry : [{}]",
                    nodes.getSelf(), currentTerm, logModule.getLast());

            preElectionTime = System.currentTimeMillis() + ThreadLocalRandom.current().nextInt(200) + 150;

            currentTerm = currentTerm + 1;
            // 推荐自己.
            votedForId = nodes.getSelf().getAdress();
            System.out.println("votedForId：" + votedForId);

            List<PeerNode> peers = nodes.getPeersWithOutSelf();

            ArrayList<Future> futureArrayList = new ArrayList<>();

            LOGGER.info("Nodes List size : {}, Nodes list info : {}", peers.size(), peers);

            // 向所有的同伴 发送请求
            for (PeerNode peer : peers) {

                futureArrayList.add(RaftThreadPool.submit(new Callable() {
                    @Override
                    public Object call() throws Exception {
                        System.out.println("222222");
                        long lastTerm = 0L;
                        LogEntry last = logModule.getLast();
                        if (last != null) {
                            lastTerm = last.getTerm();
                        }

                        ElectionTaskRequest param = ElectionTaskRequest.newBuilder().
                                term(currentTerm).
                                candidateId(nodes.getSelf().getAdress()).
                                lastLogIndex(LongConvert.convert(logModule.getLastIndex())).
                                lastLogTerm(lastTerm).
                                build();

                        Request request = Request.newBuilder()
                                .cmd(Request.REQ_VOTE)
                                .obj(param)
                                .url(peer.getAdress())
                                .build();

                        try {
                            @SuppressWarnings("unchecked")
                            Response<ElectionTaskResponse> response = getRaftRpcClient().send(request);
                            System.out.println("vated:" + response.getResult().isVoteGranted() + "    " + peer.getAdress());
                            return response;

                        } catch (RaftRemotingException e) {
                            LOGGER.error("ElectionTask RPC Fail , URL : " + request.getUrl());
                            return null;
                        }
                    }
                }));
            }

            AtomicInteger success2 = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(futureArrayList.size());

            LOGGER.info("futureArrayList.size() : {}", futureArrayList.size());
            // 等待结果.
            for (Future future : futureArrayList) {
                RaftThreadPool.submit(new Callable() {
                    @Override
                    public Object call() throws Exception {
                        try {

                            @SuppressWarnings("unchecked")
                            Response<ElectionTaskResponse> response = (Response<ElectionTaskResponse>) future.get();
                            if (response == null) {
                                return -1;
                            }
                            boolean isVoteGranted = response.getResult().isVoteGranted();

                            if (isVoteGranted) {
                                success2.incrementAndGet();
                            } else {
                                // 更新自己的任期.
                                long resTerm = response.getResult().getTerm();
                                if (resTerm >= currentTerm) {
                                    currentTerm = resTerm;
                                }
                            }
                            return 0;
                        } catch (Exception e) {
                            LOGGER.error("future.get exception , e : ", e);
                            return -1;
                        } finally {
                            latch.countDown();
                        }
                    }
                });
            }

            try {
                // 稍等片刻
                latch.await();
            } catch (InterruptedException e) {
                LOGGER.warn("InterruptedException By Master election Task");
            }

            int success = success2.get();
            LOGGER.info("node {} maybe become leader , success count = {} , status : {}", nodes.getSelf(), success, NodeStatus.Enum.value(status));
            // 如果投票期间,有其他合法的Leader(在RPCServer.hangdlerRequest()中处理的req) 发送appendEntry, 就可能变成 follower
            if (status == NodeStatus.FOLLOWER) {
                return;
            }
            // 加上自身.
            if (success >= peers.size() / 2) {
                LOGGER.warn("node {} become leader ", nodes.getSelf());
                status = LEADER;
                nodes.setLeader(nodes.getSelf());
                votedForId = "";
                becomeLeaderToDoThing();
            } else {
                // else 重新选举
                votedForId = "";
            }

        }
    }

    /**
     * 初始化所有的 nextIndex 值为自己的最后一条日志的 index + 1. 如果下次 RPC 时, 跟随者和leader 不一致,就会失败.
     * 那么 leader 尝试递减 nextIndex 并进行重试.最终将达成一致.
     */
    private void becomeLeaderToDoThing() {
        nextIndexs = new ConcurrentHashMap<>();
        matchIndexs = new ConcurrentHashMap<>();
        for (PeerNode peer : nodes.getPeersWithOutSelf()) {
            nextIndexs.put(peer, logModule.getLastIndex() + 1);
            matchIndexs.put(peer, 0L);
        }
    }


    class HeartBeatTask implements Runnable {

        @Override
        public void run() {

            if (status != LEADER) {
                return;
            }

            long current = System.currentTimeMillis();
            if (current - preHeartBeatTime < heartBeatTick) {
                return;
            }

/*
            LOGGER.info("=========== NextIndex =============");
            for (PeerNode peer : nodes.getPeersWithOutSelf()) {
                LOGGER.info("PeerNode {} nextIndex={}", peer.getAdress(), nextIndexs.get(peer));
            }
*/

            preHeartBeatTime = System.currentTimeMillis();

            // 心跳只关心 term 和 leaderID
            for (PeerNode peer : nodes.getPeersWithOutSelf()) {

                LogTaskRequest param = LogTaskRequest.newBuilder()
                        .entries(null)// 心跳,空日志.
                        .leaderId(nodes.getSelf().getAdress())
                        .serverId(peer.getAdress())
                        .term(currentTerm)
                        .build();

                Request<LogTaskRequest> request = new Request<>(
                        Request.REQ_LOG,
                        param,
                        peer.getAdress());

                RaftThreadPool.execute(() -> {
                    try {
                        Response response = getRaftRpcClient().send(request);
                        LogTaskResponse logTaskResponse = (LogTaskResponse) response.getResult();
                        long term = logTaskResponse.getTerm();

                        if (term > currentTerm) {
                            LOGGER.error("self will become follower, he's term : {}, my term : {}", term, currentTerm);
                            currentTerm = term;
                            votedForId = "";
                            status = NodeStatus.FOLLOWER;
                        }
                    } catch (Exception e) {
                        LOGGER.error("HeartBeatTask RPC Fail, request URL : {} ", request.getUrl());
                    }
                }, false);
            }
        }
    }

    @Override
    public Result addNode(PeerNode newNode) {
        return cluster.addNode(newNode);
    }

    @Override
    public Result removeNode(PeerNode oldNode) {
        return cluster.removeNode(oldNode);
    }


    public long getElectionTime() {
        return electionTime;
    }

    public void setElectionTime(long electionTime) {
        this.electionTime = electionTime;
    }

    public long getPreElectionTime() {
        return preElectionTime;
    }

    public void setPreElectionTime(long preElectionTime) {
        this.preElectionTime = preElectionTime;
    }

    public long getPreHeartBeatTime() {
        return preHeartBeatTime;
    }

    public void setPreHeartBeatTime(long preHeartBeatTime) {
        this.preHeartBeatTime = preHeartBeatTime;
    }

    public long getHeartBeatTick() {
        return heartBeatTick;
    }

    public HeartBeatTask getHeartBeatTask() {
        return heartBeatTask;
    }

    public void setHeartBeatTask(HeartBeatTask heartBeatTask) {
        this.heartBeatTask = heartBeatTask;
    }

    public ElectionTask getElectionTask() {
        return electionTask;
    }

    public void setElectionTask(ElectionTask electionTask) {
        this.electionTask = electionTask;
    }

    public ReplicationFailQueueConsumer getReplicationFailQueueConsumer() {
        return replicationFailQueueConsumer;
    }

    public void setReplicationFailQueueConsumer(ReplicationFailQueueConsumer replicationFailQueueConsumer) {
        this.replicationFailQueueConsumer = replicationFailQueueConsumer;
    }

    public LinkedBlockingQueue<ReplicationFailModel> getReplicationFailQueue() {
        return replicationFailQueue;
    }

    public void setReplicationFailQueue(LinkedBlockingQueue<ReplicationFailModel> replicationFailQueue) {
        this.replicationFailQueue = replicationFailQueue;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public Nodes getNodes() {
        return nodes;
    }

    public void setNodes(Nodes nodes) {
        this.nodes = nodes;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(long currentTerm) {
        this.currentTerm = currentTerm;
    }

    public String getVotedFor() {
        return votedForId;
    }

    public void setVotedFor(String votedFor) {
        this.votedForId = votedFor;
    }

    public LogModule getLogModule() {
        return logModule;
    }

    public void setLogModule(LogModule logModule) {
        this.logModule = logModule;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public long getLastApplied() {
        return lastApplied;
    }

    public void setLastApplied(long lastApplied) {
        this.lastApplied = lastApplied;
    }

    public Map<PeerNode, Long> getNextIndexs() {
        return nextIndexs;
    }

    public void setNextIndexs(Map<PeerNode, Long> nextIndexs) {
        this.nextIndexs = nextIndexs;
    }

    public Map<PeerNode, Long> getMatchIndexs() {
        return matchIndexs;
    }

    public void setMatchIndexs(Map<PeerNode, Long> matchIndexs) {
        this.matchIndexs = matchIndexs;
    }

    public boolean isStarted() {
        return started;
    }

    public void setStarted(boolean started) {
        this.started = started;
    }

    public NodesConfigration getConfig() {
        return config;
    }

    public static RaftRpcServer getRpcServer() {
        return RPC_SERVER;
    }

    public static void setRpcServer(RaftRpcServer raftRpcServer) {
        RPC_SERVER = raftRpcServer;
    }

    public RaftRpcClient getRaftRpcClient() {
        return raftRpcClient;
    }

    public void setRaftRpcClient(RaftRpcClient raftRpcClient) {
        this.raftRpcClient = raftRpcClient;
    }

    public StateMachine getStateMachine() {
        return stateMachine;
    }

    public void setStateMachine(StateMachine stateMachine) {
        this.stateMachine = stateMachine;
    }

    public Consensus getConsensus() {
        return consensus;
    }

    public void setConsensus(Consensus consensus) {
        this.consensus = consensus;
    }

    public ClusterMembershipChanges getDelegate() {
        return cluster;
    }

    public void setDelegate(ClusterMembershipChanges delegate) {
        this.cluster = delegate;
    }
}
