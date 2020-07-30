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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import ac.uk.ncl.gyc.raft.LogModule;
import ac.uk.ncl.gyc.raft.client.Message;
import ac.uk.ncl.gyc.raft.common.PeerNode;
import ac.uk.ncl.gyc.raft.entity.*;
import ac.uk.ncl.gyc.raft.rpc.RaftRpcClient;
import ac.uk.ncl.gyc.raft.rpc.RaftRpcClientImpl;
import ac.uk.ncl.gyc.raft.rpc.Request;
import ac.uk.ncl.gyc.raft.common.Nodes;
import com.sun.org.apache.regexp.internal.RE;
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

    /** 选举时间间隔基数 */
    public volatile long electionTime = 15 * 1000;
    /** 上一次选举时间 */
    public volatile long preElectionTime = 0;

    /** 上次一心跳时间戳 */
    public volatile long preHeartBeatTime = 0;
    /** 心跳间隔基数 */
    public final long heartBeatTick = 5 * 1000;

    public static AtomicLong LAXT_INDEX = new AtomicLong(0);


    private HeartBeatTask heartBeatTask = new HeartBeatTask();
    private ElectionTask electionTask = new ElectionTask();
    private PiggybackingTask piggybackingTask = new PiggybackingTask();

    private ReplicationFailQueueConsumer replicationFailQueueConsumer = new ReplicationFailQueueConsumer();

    private LinkedBlockingQueue<ReplicationFailModel> replicationFailQueue = new LinkedBlockingQueue<>(2048);


    /**
     * 节点当前状态
     * @see NodeStatus
     */
    public volatile int status = NodeStatus.FOLLOWER;

    public Nodes nodes;



    /* ============ 所有服务器上持久存在的 ============= */

    /** 服务器最后一次知道的任期号（初始化为 0，持续递增） */
    volatile long currentTerm = 0;
    /** 在当前获得选票的候选人的 Id */
    volatile String votedForId;
    /** 日志条目集；每一个条目包含一个用户状态机执行的指令，和收到时的任期号 */
    LogModule logModule;




    /* ============ 所有服务器上经常变的 ============= */

    /** 已知的最大的已经被提交的日志条目的索引值 */
    volatile long commitIndex;

    /** 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增) */
    volatile long lastApplied = 0;

    /* ========== 在领导人里经常改变的(选举后重新初始化) ================== */

    /** 对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一） */
    Map<PeerNode, Long> nextIndexs;

    /** 对于每一个服务器，已经复制给他的日志的最高索引值 */
    Map<PeerNode, Long> matchIndexs;



    /* ============================== */

    public volatile boolean started;

    public NodesConfigration config;

    public static RaftRpcServer RPC_SERVER;

    public RaftRpcClient raftRpcClient = new RaftRpcClientImpl();

    public StateMachine stateMachine;

    public static Map<String,String> received = new ConcurrentHashMap();

    public static Map<String,Long> startTime = new ConcurrentHashMap();

    /* ============================== */

    /** 一致性模块实现 */
    Consensus consensus;

    ClusterMembershipChanges cluster;


    /* ============================== */
    public static Map<String,AtomicInteger> extraM = new ConcurrentHashMap();

    public static Map<String,Long> latencyMap= new ConcurrentHashMap();

    public static Map<Long,CopyOnWriteArrayList<PiggybackingLog>> REQUEST_LIST= new ConcurrentHashMap();

//    public static CopyOnWriteArrayList<PiggybackingLog> REQUEST_LIST= new CopyOnWriteArrayList<>();

    public static Long SYSTEM_START_TIME = System.currentTimeMillis();
    static ReentrantLock lock = new ReentrantLock();

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
//            CCThreadPool.execute(piggybackingTask);

            RaftThreadPool.scheduleWithFixedDelay(heartBeatTask, 500);

            RaftThreadPool.scheduleAtFixedRate(electionTask, 6000, 500);
//            CCThreadPool.scheduleWithFixedDelay(piggybackingTask, 2);
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
                System.out.println("设置自身IP：" +s);
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

    public synchronized ClientResponse piggyBackingClientRequest(ClientRequest request){

        LOGGER.warn("handlerClientRequest handler {} operation,  and key : [{}], value : [{}]",
                ClientRequest.Type.value(request.getType()), request.getKey(), request.getValue());

        if (status != LEADER) {
            LOGGER.warn("Current node is not Leader , redirect to leader node, leader addr : {}, my addr : {}",
                    nodes.getLeader(), nodes.getSelf().getAdress());
            ;
            request.setRedirect(true);
            return redirect(request);
        }



        PiggybackingLog piggybackingLog = new PiggybackingLog();
        piggybackingLog.setMessage(request.getKey());
        piggybackingLog.setStartTime(System.currentTimeMillis());

        if(request.isRedirect()){
            piggybackingLog.setExtraMessage(1);
        }else {
            piggybackingLog.setExtraMessage(0);
        }

        long RUN_TIME = System.currentTimeMillis() - SYSTEM_START_TIME;
        long req_index = RUN_TIME / 2;

        CopyOnWriteArrayList<PiggybackingLog> req_list = null;


        if(REQUEST_LIST.get(req_index)!=null){
            piggybackingLog.setFirstIndex(true);
            req_list = REQUEST_LIST.get(req_index);
            req_list.add(piggybackingLog);
            REQUEST_LIST.put(req_index,req_list);
        }else{
            req_list = new CopyOnWriteArrayList();
            req_list.add(piggybackingLog);
            REQUEST_LIST.put(req_index,req_list);
        }



//        LogEntry logEntry = new LogEntry();
//        logEntry.setTerm(currentTerm);
//        logEntry.setMessage(request.getKey());
//        logEntry.setStartTime(System.currentTimeMillis());
//        logEntry.setCommand(Command.newBuilder().
//                key(request.getKey()).
//                value(request.getValue()).
//                build());
        System.out.println();
        return  ClientResponse.ok();

    }

    class PiggybackingTask implements Runnable{

        @Override
        public void run() {
//            while (true){
//                if(status!=LEADER){
//                    continue;
//                }
//
//                if(REQUEST_LIST.size()==0){
//                    continue;
//                }
//
//                long req_index = 0;
//                CopyOnWriteArrayList<PiggybackingLog> req_list = null;
//                for (Map.Entry<Long, CopyOnWriteArrayList<PiggybackingLog>> entry : REQUEST_LIST.entrySet()) {
//                    req_index = entry.getKey();
//                    req_list = entry.getValue();
//                    if (req_list != null) {
//                        break;
//                    }
//                }
//                long RUN_TIME = System.currentTimeMillis() - SYSTEM_START_TIME;
//
//                long cur_index = RUN_TIME / 2;
//
//                if(req_index==cur_index) {
//                    continue;
//                }
//
//                REQUEST_LIST.remove(req_index);
//
//
//                final CopyOnWriteArrayList<PiggybackingLog> log_list = req_list;
//
//                CCThreadPool.execute(new Runnable() {
//
//                    @Override
//                    public void run() {
//                        System.out.println("____________________________________________________________");
//                        System.out.println("pigg1111Task  start: ");
//                        List<LogEntry> messages = new ArrayList<>();
//                        PiggybackingLog firstLog = null;
//
//                        int extra_message = 6;
//
//
//
//                        for(int i=0; i < log_list.size(); i++ ){
//                            PiggybackingLog log = log_list.get(i);
//                            System.out.println("piggTask  message: "+log.getMessage());
//                            LogEntry logEntry = new LogEntry();
//                            logEntry.setTerm(currentTerm);
//                            logEntry.setMessage(log.getMessage());
//                            logEntry.setStartTime(System.currentTimeMillis());
//                            logEntry.setCommand(Command.newBuilder().
//                                    key(log.getMessage()).
//                                    value("").
//                                    build());
//
//                            if(log.isFirstIndex()){
//                                logEntry.setFirstIndex(true);
//                                firstLog = log;
//                            }
//
//                            extra_message= extra_message+log.getExtraMessage();
//                            messages.add(logEntry);
//
//                        }
//
//                        boolean reqEntry = handlerClientRequest(messages);
//
//                        if(reqEntry){
//                            long latency = System.currentTimeMillis() - firstLog.getStartTime();
//
//
//                            boolean reqCommit = ReqCommit(messages);
//
//                            if(reqCommit){
//                                long followerLatency = 0;
//
//                                for(long a :latencyMap.get(firstLog.getMessage())){
//                                    followerLatency = followerLatency+a;
//                                }
//                                followerLatency = followerLatency/latencyMap.get(firstLog.getMessage()).size();
//
//                                Message message = new Message(firstLog.getMessage(),extra_message,latency,followerLatency);
//                                System.out.println("leaderLatency :      "+latency);
//                                System.out.println("followerLatency :      "+followerLatency);
//                                System.out.println("extra_message :      "+extra_message);
//
//                                List<String> mmmm = new ArrayList<>();
//                                System.out.println("messages size:      "+messages.size());
//
//                                for(int i=0; i < messages.size(); i++){
//                                    mmmm.add(messages.get(i).getMessage());
//                                    System.out.println("Current PIGG messages :      "+messages.get(i).getMessage());
//                                }
//                                message.setMessages(mmmm);
//
//                                resultJson.write(message);
//
//                                System.out.println("piggybacking request successful: firstReq :"+ firstLog.getMessage()+", req count: "+messages.size() );
//
//
//
//                            }
//                        }
//                    }
//                });
//
//
//
//            }
//
//
        }
    }



    /**
     * 客户端的每一个请求都包含一条被复制状态机执行的指令。
     * 领导人把这条指令作为一条新的日志条目附加到日志中去，然后并行的发起附加条目 RPCs 给其他的服务器，让他们复制这条日志条目。
     * 当这条日志条目被安全的复制（下面会介绍），领导人会应用这条日志条目到它的状态机中然后把执行的结果返回给客户端。
     * 如果跟随者崩溃或者运行缓慢，再或者网络丢包，
     *  领导人会不断的重复尝试附加日志条目 RPCs （尽管已经回复了客户端）直到所有的跟随者都最终存储了所有的日志条目。
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
            received.put(request.getKey(),nodes.getSelf().getAdress());
            startTime.put(request.getKey(),receiveTime);
            request.setRedirect(true);
            request.setSentAdd(nodes.getSelf().getAdress());
            return redirect(request);
        }

        //record extra messages
//        if(status == LEADER && extraM.get(request.getKey())==null){
//            if(request.isRedirect()){
//                extraM.put(request.getKey(),new AtomicInteger(1));
//            }else {
//                extraM.put(request.getKey(),new AtomicInteger(0));
//            }
//
//            latencyMap.put(request.getKey(),new CopyOnWriteArrayList<>());
//        }


//
//        if (request.getType() == ClientRequest.GET) {
//            LogEntry logEntry = stateMachine.get(request.getKey());
//            if (logEntry != null) {
//                return new ClientResponse(logEntry.getCommand());
//            }
//            return new ClientResponse(null);
//        }



        PiggybackingLog piggybackingLog = new PiggybackingLog();
        piggybackingLog.setMessage(request.getKey());
        piggybackingLog.setStartTime(receiveTime);
        if(request.isRedirect()){
            received.put(request.getKey(),request.getSentAdd());
            piggybackingLog.setExtraMessage(1);
        }else {
            received.put(request.getKey(),nodes.getSelf().getAdress());
            request.setSentAdd(nodes.getSelf().getAdress());
            piggybackingLog.setExtraMessage(0);
        }

        piggybackingLog.setSentAddr(request.getSentAdd());

        startTime.put(request.getKey(),receiveTime);

        long RUN_TIME = receiveTime - SYSTEM_START_TIME;
        long req_index = RUN_TIME / 2;

        System.out.println("cur_time" + req_index );


        CopyOnWriteArrayList<PiggybackingLog> req_list = null;
        PiggybackingLog firstPiggy = null;

        if(REQUEST_LIST.get(req_index)!=null){
            System.out.println("cur_req" + request.getKey()+" is  not first p" );
            req_list = REQUEST_LIST.get(req_index);
            req_list.add(piggybackingLog);
            REQUEST_LIST.put(req_index,req_list);
        }else{
            System.out.println("cur_req" + request.getKey()+" is first p          req_index"+ req_index );
            piggybackingLog.setFirstIndex(true);
            req_list = new CopyOnWriteArrayList();
            req_list.add(piggybackingLog);
            REQUEST_LIST.put(req_index,req_list);
        }
        System.out.println("last_index   " + LAXT_INDEX.get() );

        if(req_index<=LAXT_INDEX.get()){
            System.out.println("return 1111111 size" );
            return ClientResponse.ok();
        }

        long last_index = LAXT_INDEX.get();

        LAXT_INDEX.getAndSet(req_index);
        if(last_index == 0){
            System.out.println("return 3333333 size" );

//        cur_index = req_index;
            return ClientResponse.ok();
        }

        if(REQUEST_LIST.get(last_index)==null){
            System.out.println("return 2222222 size" );
            return ClientResponse.ok();
        }else{
            req_list = REQUEST_LIST.get(last_index);
        }

        REQUEST_LIST.remove(last_index);


        System.out.println("req_list size" + req_list.size());
        for(PiggybackingLog p : req_list){
            if(p.isFirstIndex()){
                firstPiggy = p;
            }
        }

        System.out.println("FIST PPP " + firstPiggy.getMessage());


        int extra_message = 6;
        List<LogEntry> logEntries = new ArrayList<>();
        LogEntry firstLog = null;
        for(PiggybackingLog log : req_list){
            LogEntry logEntry = new LogEntry();
            logEntry.setTerm(currentTerm);
            logEntry.setMessage(log.getMessage());
            logEntry.setStartTime(log.getStartTime());
            logEntry.setSentAddr(log.getSentAddr());

            logEntry.setCommand(Command.newBuilder().
                    key(log.getMessage()).
                    value("").
                    build());

            if(log.isFirstIndex()){
                logEntry.setFirstIndex(true);
                firstLog = logEntry;
            }

            extra_message= extra_message+log.getExtraMessage();
            // 预提交到本地日志, TODO 预提交
            logModule.write(logEntry);
            logEntries.add(logEntry);
            LOGGER.info("write logModule success, logEntry info : {}, log index : {}", logEntry, logEntry.getIndex());
        }



        final AtomicInteger success = new AtomicInteger(0);

        List<Future<Boolean>> futureList = new CopyOnWriteArrayList<>();

        int count = 0;
        //  复制到其他机器
        for (PeerNode peer : nodes.getPeersWithOutSelf()) {
            // TODO check self and CCThreadPool
            count++;
            // 并行发起 RPC 复制.
            futureList.add(replication(peer, logEntries));
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



        //  响应客户端(成功一半)
       if (success.get() >= count) {
            long latency = System.currentTimeMillis() - firstLog.getStartTime();
           for(LogEntry lls : logEntries){
               if(lls.getSentAddr().equals(nodes.getSelf().getAdress())){
                   latencyMap.put(lls.getMessage(),System.currentTimeMillis() - startTime.get(lls.getMessage()));
               }
           }
            boolean reqCommit = ReqCommit(logEntries);

            if(reqCommit==true){

                for(LogEntry ll : logEntries){
                    // 更新
                    commitIndex = ll.getIndex();
                    //  应用到状态机
                    getStateMachine().apply(ll);
                    lastApplied = commitIndex;
                }

                LOGGER.info("success apply local state machine,  logEntry info : {}", firstLog);


                List<Message> requests = new ArrayList<>();
                for(LogEntry lll : logEntries){
                    String lmmm =  lll.getMessage();
                   Message me  = new Message();
                   me.setMessage(lmmm);

                   if(lll.isFirstIndex()){
                       me.setExtra_message(extra_message);
                   }else{
                       me.setExtra_message(0);
                   }

                    if(lll.getSentAddr().equals(nodes.getSelf().getAdress())){
                        me.setLeader_latency(latencyMap.get(lmmm));
                        me.setFollower_latency(0);
                    }else{
                        me.setLeader_latency(0);
                        me.setFollower_latency(latencyMap.get(lmmm));
                    }

                    System.out.print(lmmm+ " has been commit(PIGG)__latency: "+latencyMap.get(lmmm));
                    requests.add(me);
                    received.remove(lmmm);
                    startTime.remove(lmmm);
                    latencyMap.remove(lmmm);
                }


                System.out.println("");
                System.out.println("requests count: "+ requests.size());

                ClientResponse clientResponse = new ClientResponse();

//                if(request.isRedirect()){
//                    clientResponse.setExtraMessageCount(7);
//                }else {
//                    clientResponse.setExtraMessageCount(6);
//                }
                clientResponse.setExtraMessageCount(extra_message);

                clientResponse.setRequests(requests);
                clientResponse.setResult("ok");

                extraM.remove(request.getKey());

                return clientResponse;
            }else{
                return ClientResponse.fail();
            }


      }
//else {
//            logModule.removeOnStartIndex(logEntry.getIndex());
//            LOGGER.warn("fail apply local state  machine,  logEntry info : {}", logEntry);
//            // TODO 不应用到状态机,但已经记录到日志中.由定时任务从重试队列取出,然后重复尝试,当达到条件时,应用到状态机.
//            // 这里应该返回错误, 因为没有成功复制过半机器.
//            return ClientResponse.fail();
//        }
        return ClientResponse.fail();

    }

    private boolean ReqCommit(List<LogEntry> logEntries){

        final AtomicInteger success = new AtomicInteger(0);

        List<Future<Boolean>> futureList = new CopyOnWriteArrayList<>();

        int count = 0;
        //  复制到其他机器
        for (PeerNode peer : nodes.getPeersWithOutSelf()) {
            // TODO check self and CCThreadPool
            count++;

            futureList.add(Commit(peer,logEntries));
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
        }else{
            return false;
        }
    }

    public Future<Boolean> Commit(PeerNode peer, List<LogEntry> lll) {


        return RaftThreadPool.submit(new Callable() {
            @Override
            public Boolean call() throws Exception {


                long start = System.currentTimeMillis(), end = start;



                LogEntry firstLog = null;

                Map<String,String> newM= new HashMap<>();
                for (LogEntry logEntry : lll) {
                    newM.put(logEntry.getMessage(),logEntry.getSentAddr());
                    if (logEntry.isFirstIndex()) {
                        firstLog = logEntry;
                    }
                }

                String key = firstLog.getMessage();

                CommitRequest commitRequest = new CommitRequest();
                commitRequest.setMessage(firstLog.getMessage());
                commitRequest.setTerm(currentTerm);
                commitRequest.setServerId(peer.getAdress());
                commitRequest.setLogEntries(lll);
                commitRequest.setNewMap(newM);

                Request request = Request.newBuilder()
                        .cmd(Request.REQ_COMMIT)
                        .obj(commitRequest)
                        .url(peer.getAdress())
                        .build();

//                    AtomicInteger extraMCount = extraM.get(key);
//                    extraMCount.incrementAndGet();
//                    extraM.put(key,extraMCount);

                try {
                    Response response = getRaftRpcClient().send(request);
                    if (response == null) {

                    }
                    CommitResponse result = (CommitResponse) response.getResult();

                    if (result != null && result.isSuccess()) {
                        LOGGER.info("commit success , follower=[{}], entry=[{}]", peer, key);
                        Map<String, Long> latencyM = result.getLatency();

                        for(Map.Entry<String,Long> entr : latencyM.entrySet()){
                            latencyMap.put(entr.getKey(),entr.getValue());
                        }

                        return true;
                    }

                    end = System.currentTimeMillis();

                } catch (Exception e) {

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
                    } catch (CancellationException  | ExecutionException | InterruptedException e) {
                        e.printStackTrace();
                        resultList.add(false);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
    }


    /** 复制到其他机器  */
    public Future<Boolean> replication(PeerNode peer, List<LogEntry> entries) {

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
                    LogEntry entry = null;
                    long en_index = 0;
                    for(int j = 0 ; j<entries.size();j++){
                        if(j==0){
                            en_index =entries.get(j).getIndex();
                            entry = entries.get(j);
                        }else{
                            if(entries.get(j).getIndex()<en_index){
                                en_index = entries.get(j).getIndex();
                                entry = entries.get(j);
                            }
                        }
                    }

                    Long nextIndex = nextIndexs.get(peer);
                    LinkedList<LogEntry> logEntries = new LinkedList<>();

//                    System.out.println("entry   "+entry);
//                    System.out.println("(entry.getIndex()   "+entry.getIndex());
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

                    logTaskRequest.setEntries(entries);

                    Request request = Request.newBuilder()
                        .cmd(Request.REQ_LOG)
                        .obj(logTaskRequest)
                        .url(peer.getAdress())
                        .build();

//                    AtomicInteger extraMCount = extraM.get(entry.getCommand().getKey());
//                    extraMCount.incrementAndGet();
//                    extraM.put(entry.getCommand().getKey(),extraMCount);


                    try {
                        Response response = getRaftRpcClient().send(request);
                        if (response == null) {
                            return false;
                        }
                        LogTaskResponse result = (LogTaskResponse) response.getResult();
                        if (result != null && result.isSuccess()) {
                            LOGGER.info("append follower entry success , follower=[{}], entry=[{}]", peer, logTaskRequest.getEntries());
                            // update 这两个追踪值
                            nextIndexs.put(peer, entry.getIndex() + entries.size());
                            matchIndexs.put(peer, entry.getIndex()+ entries.size()-1);

/*                            AtomicInteger mm = extraM.get(entry.getCommand().getKey());
                            mm.incrementAndGet();
                            extraM.put(entry.getCommand().getKey(),mm);*/
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

        /** 一分钟 */
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
     *      自增当前的任期号（currentTerm）
     *      给自己投票
     *      重置选举超时计时器
     *      发送请求投票的 RPC 给其他所有服务器
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
            System.out.println("votedForId：" +votedForId);

            List<PeerNode> peers = nodes.getPeersWithOutSelf();

            ArrayList<Future> futureArrayList = new ArrayList<>();

            LOGGER.info("Nodes List size : {}, Nodes list info : {}", peers.size(), peers);

            // 向所有的同伴 发送请求
            for (PeerNode peer : peers) {

                futureArrayList.add(RaftThreadPool.submit(new Callable() {
                    @Override
                    public Object call() throws Exception {
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
