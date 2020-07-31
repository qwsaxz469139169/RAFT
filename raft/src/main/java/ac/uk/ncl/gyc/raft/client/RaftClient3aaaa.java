package ac.uk.ncl.gyc.raft.client;

import ac.uk.ncl.gyc.raft.current.RaftThreadPool;
import ac.uk.ncl.gyc.raft.rpc.RaftRpcClient;
import ac.uk.ncl.gyc.raft.rpc.RaftRpcClientImpl;
import ac.uk.ncl.gyc.raft.rpc.Request;
import ac.uk.ncl.gyc.raft.rpc.Response;
import com.alibaba.fastjson.JSON;
import com.alipay.remoting.exception.RemotingException;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by GYC on 2020/6/2.
 */
public class RaftClient3aaaa {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftClient2.class);


    private final static RaftRpcClient client = new RaftRpcClientImpl();
    private static List<Message> messages = new CopyOnWriteArrayList<>();

    private static  AtomicInteger MINDEX = new AtomicInteger(0);
    private static AtomicLong count = new AtomicLong(3);
    private static final int delay= 1000;
    private static final int endcount= 90000;
    private static final int arriveRate = 150;

    static List<String> nodelist = Lists.newArrayList("100.70.49.128:8775", "100.70.49.85:8776", "100.70.48.5:8777");
    public static void main(String[] args) throws RemotingException, InterruptedException {
        MyTimerTask myTimerTask = new MyTimerTask();
        RaftThreadPool.scheduleWithFixedDelay(myTimerTask,delay);

        while (true){
            if(messages.size()>endcount){
                String s = JSON.toJSONString(messages);
                FileWriter fw = null;
                File f = new File("D:/0.75_case1Raft1.txt");
                try {
                    if(!f.exists()){
                        f.createNewFile();
                    }
                    fw = new FileWriter(f);
                    BufferedWriter out = new BufferedWriter(fw);
                    out.write(s, 0, s.length()-1);
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println("end");
                System.exit(0);
            }
        }





    }


    public MyTimerTask getMyTimerTask(){
        return  new MyTimerTask();
    }

    static class MyTimerTask implements Runnable{

        @Override
        public void run() {
            for (int i = 0; i < arriveRate; i++) {
                int m = MINDEX.addAndGet(1);
                int index = (int) (count.incrementAndGet() % nodelist.size());
                String req_address = nodelist.get(index);

                ClientRequest obj = ClientRequest.newBuilder().key("client1:" + m ).value("world:").type(ClientRequest.PUT).build();

                Request<ClientRequest> r = new Request<>();
                r.setObj(obj);
                r.setUrl(req_address);
                r.setCmd(Request.REQ_CLIENT);

                Response<ClientResponse> response;

                try {
                    response = client.send(r);
                    ClientResponse clientResponse = response.getResult();
                    LOGGER.info("request content : {}, extra message : {}, leader latency: {}, follower latency: {}", obj.key, clientResponse.getExtraMessageCount(), clientResponse.getLeaderLatency(), clientResponse.getFollowerLatency());
                    Message message1 = new Message(obj.key, clientResponse.getExtraMessageCount(), clientResponse.getLeaderLatency(), clientResponse.getFollowerLatency());
                    messages.add(message1);

                } catch (Exception e) {

                }


            }
        }
    }
}