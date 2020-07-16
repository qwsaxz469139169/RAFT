package ac.uk.ncl.gyc.raft.client;

import ac.uk.ncl.gyc.raft.impl.ResultJson;
import ac.uk.ncl.gyc.raft.membership.changes.Result;

/**
 * Created by GYC on 2020/7/16.
 */
public class Test {
    public static void main(String[] args) {
        long SYSTEM_START_TIME = System.currentTimeMillis();

        while (true){
            long RUN_TIME = System.currentTimeMillis() - SYSTEM_START_TIME;
            long req_index = RUN_TIME / 2;
            System.out.println(req_index);
        }

    }
}
