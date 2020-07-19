package ac.uk.ncl.gyc.raft.client;

import ac.uk.ncl.gyc.raft.rpc.RaftRpcClient;
import ac.uk.ncl.gyc.raft.rpc.RaftRpcClientImpl;
import ac.uk.ncl.gyc.raft.rpc.Request;
import ac.uk.ncl.gyc.raft.rpc.Response;
import com.alipay.remoting.exception.RemotingException;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by GYC on 2020/6/2.
 */
public class PiggRaftClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(PiggRaftClient.class);


    private final static RaftRpcClient client = new RaftRpcClientImpl();

    static String req_address = "";
    static List<String> nodelist = Lists.newArrayList("localhost:8775", "localhost:8776", "localhost:8777");

    public static void main(String[] args) throws RemotingException, InterruptedException {

        AtomicLong count = new AtomicLong(3);
            try {
                int index = (int) (count.incrementAndGet() % nodelist.size());
                req_address = nodelist.get(index);

                ClientRequest obj = ClientRequest.newBuilder().key("hello:").value("world:").type(ClientRequest.PUT).build();

                Request<ClientRequest> r = new Request<>();
                r.setObj(obj);
                r.setUrl(req_address);
                r.setCmd(Request.REQ_CLIENT);

                Response<ClientResponse> response;

                try {
                    response = client.send(r);
                    if(response.getResult()!=null){
                        ClientResponse clientResponse = response.getResult();
                        System.out.println("message : "+obj+ " send successful!");
                        if(clientResponse.getRequests()!=null){
                            for(String s :clientResponse.getRequests()){
                                System.out.println("message : "+s+ " commit!!!!!!!!!!!");
                            }
                            System.out.println("Follower Latency: "+clientResponse.getFollowerLatency()+", Leader Latency: "+clientResponse.getLeaderLatency()+", extra message: "+ clientResponse.getExtraMessageCount());
                        }

                    }



                } catch (Exception e) {

                }

            } catch (Exception e) {
                e.printStackTrace();
            }




    }
}
