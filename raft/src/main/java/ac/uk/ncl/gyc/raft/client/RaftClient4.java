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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by GYC on 2020/6/2.
 */
public class RaftClient4 {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftClient4.class);


    private final static RaftRpcClient client = new RaftRpcClientImpl();
    private static List<Message> messages = new CopyOnWriteArrayList<>();

    static List<String> nodelist = Lists.newArrayList("100.70.49.128:8775", "100.70.49.85:8776", "100.70.48.5:8777");    //   static List<String> nodelist = Lists.newArrayList("localhost:8775", "localhost:8776", "localhost:8777");
    //   static List<String> nodelist = Lists.newArrayList("localhost:8775", "localhost:8776", "localhost:8777");

    public static void main(String[] args) throws RemotingException, InterruptedException {

        AtomicLong count = new AtomicLong(3);

        int message = 0;
       for(int j =0; j<605; j++){
            for(int i=0;i<5;i++){
                message = message+1;
                int m = message;
                int index = (int) (count.incrementAndGet() % nodelist.size());
                String req_address = nodelist.get(index);

                RaftThreadPool.execute(new Runnable() {
                    @Override
                    public void run() {

                        ClientRequest obj = ClientRequest.newBuilder().key("client2:"+m).value("world:").type(ClientRequest.PUT).build();

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
                                    for(String s :clientResponse.getRequests()){
                                        con++;
                                        System.out.println("message : "+s+ " commit!!!!!!!!!!!");
                                    }
                                    System.out.println("Follower Latency: "+clientResponse.getFollowerLatency()+", Leader Latency: "+clientResponse.getLeaderLatency()+", extra message: "+ clientResponse.getExtraMessageCount());
                                    Message message1 = new Message(con, clientResponse.getExtraMessageCount(), clientResponse.getLeaderLatency(), clientResponse.getFollowerLatency());
                                     messages.add(message1);
                                }

                            }


                        } catch (Exception e) {

                        }
                    }
                });

           }
            Thread.sleep(1000);
        }


        String s = JSON.toJSONString(messages);
        FileWriter fw = null;
        File f = new File("D:/5_case2Raft1.txt");
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



    }
}
