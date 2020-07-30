package ac.uk.ncl.gyc.raft.entity;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by GYC on 2020/6/27.
 */
public class CommitResponse  implements Serializable {

    boolean success;

    Map<String,Long> latency;

    public Map<String,Long> getLatency() {
        return latency;
    }

    public void setLatency(Map<String,Long> latency) {
        this.latency = latency;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }
}
