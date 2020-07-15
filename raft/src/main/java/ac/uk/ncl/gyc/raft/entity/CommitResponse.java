package ac.uk.ncl.gyc.raft.entity;

import java.io.Serializable;

/**
 * Created by GYC on 2020/6/27.
 */
public class CommitResponse  implements Serializable {

    boolean success;

    long latency;

    public long getLatency() {
        return latency;
    }

    public void setLatency(long latency) {
        this.latency = latency;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }
}
