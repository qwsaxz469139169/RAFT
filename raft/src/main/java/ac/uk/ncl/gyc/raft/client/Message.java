package ac.uk.ncl.gyc.raft.client;

import com.alibaba.fastjson.annotation.JSONField;

import java.io.Serializable;

/**
 * Created by GYC on 2020/7/5.
 */
public class Message implements Serializable {

    @JSONField(name = "message")
    private String message;

    @JSONField(name = "leader_latency")
    private long leader_latency;

    @JSONField(name = "follower_latency")
    private long follower_latency;
    public Message(){

    }

    public Message(String message, long leader_latency, long follower_latency) {
        this.message = message;

        this.leader_latency = leader_latency;
        this.follower_latency = follower_latency;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }


    public long getLeader_latency() {
        return leader_latency;
    }

    public void setLeader_latency(long leader_latency) {
        this.leader_latency = leader_latency;
    }

    public long getFollower_latency() {
        return follower_latency;
    }

    public void setFollower_latency(long follower_latency) {
        this.follower_latency = follower_latency;
    }
}
