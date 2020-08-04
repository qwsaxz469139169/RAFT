package ac.uk.ncl.gyc.raft.client;

import ac.uk.ncl.gyc.raft.clientCur.CCThreadPool;
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
public class CCRaftClient4 {

    private static final Logger LOGGER = LoggerFactory.getLogger(CCRaftClient4.class);


    private final static RaftRpcClient client = new RaftRpcClientImpl();
    private static List<Message> messages = new CopyOnWriteArrayList<>();
    private static  AtomicLong count = new AtomicLong(3);
    private static  AtomicLong receiveCount = new AtomicLong(0);
    private static AtomicInteger m_index = new AtomicInteger(0);
    //static List<String> nodelist = Lists.newArrayList("100.70.48.36:8775", "100.70.49.21:8776", "100.70.49.86:8777");
    static List<String> nodelist = Lists.newArrayList("localhost:8775", "localhost:8776", "localhost:8777");

    private static final int clientNum = 3;
    private static final int runtime= 620;
    private static final int c = 2;
    private static final int delay= 40;
    private static final int endcount= 100;
    private static final int arriveRate = 15;
    private static final String arriveRateNum = "test";


    public static void main(String[] args) throws RemotingException, InterruptedException {

        MyTask myTask = new MyTask();
        CCThreadPool.scheduleWithFixedDelayUs(myTask,delay);

        while (true){
            if(receiveCount.get()>endcount){
                String s = JSON.toJSONString(messages);
                FileWriter fw = null;
                File f = new File("D:/"+arriveRateNum+"_case"+c+"Raft"+clientNum+".txt");
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
                System.out.println("end messages.size():" +messages.size()+", bunching: "+receiveCount.get());
                System.exit(0);
            }
        }



    }

    static class MyTask implements Runnable{

        @Override
        public void run() {
            int m = m_index.addAndGet(1);
            int index = (int) (count.incrementAndGet() % nodelist.size());
            String req_address = nodelist.get(index);
            ClientRequest obj = ClientRequest.newBuilder().key("client"+clientNum+":"+m).value("world:").type(ClientRequest.PUT).build();

            Request<ClientRequest> r = new Request<>();
            r.setObj(obj);
            r.setUrl(req_address);
            r.setCmd(Request.REQ_CLIENT);
            Response<ClientResponse> response;

            try {
                response = client.send(r);
                if(response.getResult()!=null){
                    ClientResponse clientResponse = response.getResult();
                    System.out.println("message : "+obj.getKey()+ " send successful!");
                    if(clientResponse.getRequests()!=null){
                        int con = 0;

                        for(Message s :clientResponse.getRequests()){

                            con++;
                            if(s.getFollower_latency()==0){
                                System.out.println("message : "+s.getMessage()+ " commit! Leader latency: "+s.getLeader_latency());
                            }else{
                                System.out.println("message : "+s.getMessage()+ " commit! Follower latency: "+s.getFollower_latency());
                            }

                            messages.add(s);
                        }
                        receiveCount.addAndGet(con);
                        System.out.println("Cur commit message count: "+con+", extra message: "+ clientResponse.getExtraMessageCount());
                    }

                }
            }catch (Exception e){
                e.printStackTrace();
                String s = JSON.toJSONString(messages);
                FileWriter fw = null;
                File f = new File("D:/"+arriveRateNum+"_case"+c+"Raft"+clientNum+".txt");
                try {
                    if(!f.exists()){
                        f.createNewFile();
                    }
                    fw = new FileWriter(f);
                    BufferedWriter out = new BufferedWriter(fw);
                    out.write(s, 0, s.length()-1);
                    out.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
                System.out.println("end messages.size():" +messages.size());
                System.exit(0);
            }

        }
    }
}
