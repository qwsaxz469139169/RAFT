package ac.uk.ncl.gyc.raft.client;

import ac.uk.ncl.gyc.raft.clientCur.CCThreadPool;
import ac.uk.ncl.gyc.raft.current.RaftThreadPool;

/**
 * Created by GYC on 2020/7/30.
 */
public class Twzt {
    public static void main(String[] args) {

        RaftThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                System.out.print("1");
            }
        });

        CCThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                System.out.print("2");
            }
        });
    }

}

