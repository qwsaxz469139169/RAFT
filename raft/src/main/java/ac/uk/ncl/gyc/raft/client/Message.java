package ac.uk.ncl.gyc.raft.client;

import com.alibaba.fastjson.annotation.JSONField;

import java.io.Serializable;
import java.util.List;

/**
 * Created by GYC on 2020/7/5.
 */
public class Message implements Serializable{

    @JSONField(name = "message")
    private String message;

    @JSONField(name = "extra_message")
    private int extra_message;

    @JSONField(name = "leader_latency")
    private long leader_latency;

    @JSONField(name = "follower_latency")
    private long follower_latency;

    private List<String> messages;

    public Message(){}

    public Message(String message, int extra_message, long leader_latency, long follower_latency) {
        this.message = message;
        this.extra_message = extra_message;
        this.leader_latency = leader_latency;
        this.follower_latency = follower_latency;
    }

    public List<String> getMessages() {
        return messages;
    }

    public void setMessages(List<String> messages) {
        this.messages = messages;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public int getExtra_message() {
        return extra_message;
    }

    public void setExtra_message(int extra_message) {
        this.extra_message = extra_message;
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
