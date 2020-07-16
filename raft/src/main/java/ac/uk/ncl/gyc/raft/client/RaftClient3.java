package ac.uk.ncl.gyc.raft.client;

import ac.uk.ncl.gyc.raft.current.RaftThreadPool;
import ac.uk.ncl.gyc.raft.current.SleepHelper;
import ac.uk.ncl.gyc.raft.entity.LogEntry;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by GYC on 2020/6/2.
 */
public class RaftClient3 {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftClient2.class);


    private final static RaftRpcClient client = new RaftRpcClientImpl();
    private static List<Message> messages = new CopyOnWriteArrayList<>();

//    static List<String> nodelist = Lists.newArrayList("100.70.49.99:8775", "100.70.49.28:8776", "100.70.49.44:8777");
    static List<String> nodelist = Lists.newArrayList("localhost:8775", "localhost:8776", "localhost:8777");

    public static void main(String[] args) throws RemotingException, InterruptedException {

        AtomicLong count = new AtomicLong(3);

        int message = 0;
        for(int j =0; j<5; j++){
            for(int i=0;i<5;i++){
                message = message+1;
                int m = message;
                int index = (int) (count.incrementAndGet() % nodelist.size());
                String req_address = nodelist.get(index);

                RaftThreadPool.execute(new Runnable() {
                    @Override
                    public void run() {

                        ClientRequest obj = ClientRequest.newBuilder().key("client222:"+m).value("world:").type(ClientRequest.PUT).build();

                        Request<ClientRequest> r = new Request<>();
                        r.setObj(obj);
                        r.setUrl(req_address);
                        r.setCmd(Request.REQ_CLIENT);

                        Response<ClientResponse> response;

                        try {
                            response = client.send(r);
//                            ClientResponse clientResponse = response.getResult();
//                            LOGGER.info("request content : {}, extra message : {}, leader latency: {}, follower latency: {}", obj.key, clientResponse.getExtraMessageCount(),clientResponse.getLeaderLatency(),clientResponse.getFollowerLatency());
//                            Message message1 = new Message(obj.key, clientResponse.getExtraMessageCount(), clientResponse.getLeaderLatency(), clientResponse.getFollowerLatency());
//                            messages.add(message1);
                            LOGGER.info("request content : {} "+ obj.key);


                        } catch (Exception e) {

                        }
                    }
                });

            }
            Thread.sleep(1000);
        }

//
//        String s = JSON.toJSONString(messages);
//        FileWriter fw = null;
//        File f = new File("D:/raft_1.txt");
//        try {
//            if(!f.exists()){
//                f.createNewFile();
//            }
//            fw = new FileWriter(f);
//            BufferedWriter out = new BufferedWriter(fw);
//            out.write(s, 0, s.length()-1);
//            out.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        System.out.println("end");



    }
}
