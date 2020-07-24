package ac.uk.ncl.gyc.raft;

import java.util.Arrays;

import ac.uk.ncl.gyc.raft.common.NodesConfigration;
import ac.uk.ncl.gyc.raft.impl.NodeImpl;

/**
 * -DserverPort=8775
 * -DserverPort=8776
 * -DserverPort=8777
 * -DserverPort=8778
 * -DserverPort=8779
 */
public class RaftBootStrap {

    public static void main(String[] args) throws Throwable {
        main0(args);
    }

    public static void main0(String[] args) throws Throwable {

       //String[] peerAddr = {"192.168.199.101:8775","192.168.199.102:8775","192.168.199.103:8775"};
        String[] peerAddr = {"100.70.49.128:8775", "100.70.49.85:8776", "100.70.49.226:8777"};
//        String[] peerAddr = {"localhost:8775","localhost:8776","localhost:8777"};

        NodesConfigration config = new NodesConfigration();

        // 自身节点
        config.setSelfPort(Integer.valueOf(System.getProperty("serverPort")));
//       config.setSelfPort(Integer.valueOf(args[0]));

        // 其他节点地址
        config.setPeerAddrs(Arrays.asList(peerAddr));

        Node node = NodeImpl.getInstance();
        node.setConfig(config);

        node.init();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                node.destroy();
            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }
        }));

    }

}
