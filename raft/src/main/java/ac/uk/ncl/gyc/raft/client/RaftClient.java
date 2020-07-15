package ac.uk.ncl.gyc.raft.client;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.alipay.remoting.exception.RemotingException;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ac.uk.ncl.gyc.raft.current.SleepHelper;
import ac.uk.ncl.gyc.raft.entity.LogEntry;
import ac.uk.ncl.gyc.raft.rpc.RaftRpcClientImpl;
import ac.uk.ncl.gyc.raft.rpc.Request;
import ac.uk.ncl.gyc.raft.rpc.Response;
import ac.uk.ncl.gyc.raft.rpc.RaftRpcClient;

public class RaftClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftClient.class);


    private final static RaftRpcClient client = new RaftRpcClientImpl();

    static String req_address = "";
    static List<String> nodelist = Lists.newArrayList("localhost:8775", "localhost:8776", "localhost:8777");

    public static void main(String[] args) throws RemotingException, InterruptedException {

        AtomicLong count = new AtomicLong(3);

        for (int i = 3; ; i++) {
            try {
                int index = (int) (count.incrementAndGet() % nodelist.size());
                req_address = nodelist.get(index);

                ClientRequest obj = ClientRequest.newBuilder().key("hello:" + i).value("world:" + i).type(ClientRequest.PUT).build();

                Request<ClientRequest> r = new Request<>();
                r.setObj(obj);
                r.setUrl(req_address);
                r.setCmd(Request.REQ_CLIENT);
                Response<String> response;
                try {
                    response = client.send(r);
                } catch (Exception e) {
                    r.setUrl(nodelist.get((int) ((count.incrementAndGet()) % nodelist.size())));
                    response = client.send(r);
                }

                LOGGER.info("request content : {}, url : {}, put response : {}", obj.key + "=" + obj.getValue(), r.getUrl(), response.getResult());

                SleepHelper.sleep(1000);

                obj = ClientRequest.newBuilder().key("hello:" + i).type(ClientRequest.GET).build();

                req_address = nodelist.get(index);

                r.setUrl(req_address);
                r.setObj(obj);

                Response<LogEntry> response2;
                try {
                    response2 = client.send(r);
                } catch (Exception e) {
                    r.setUrl(nodelist.get((int) ((count.incrementAndGet()) % nodelist.size())));
                    response2 = client.send(r);
                }

                LOGGER.info("request content : {}, url : {}, get response : {}", obj.key + "=" + obj.getValue(), r.getUrl(), response2.getResult());
            } catch (Exception e) {
                e.printStackTrace();
                i = i - 1;
            }

            SleepHelper.sleep(5000);
        }


    }

}
