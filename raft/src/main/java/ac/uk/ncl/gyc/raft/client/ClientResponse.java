package ac.uk.ncl.gyc.raft.client;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class ClientResponse implements Serializable {

    Object result;


    int extraMessageCount;

    int message_count;

    Map<String,Long> leaderLatency;

    Map<String,Long> followerLatency;

    List<Message> messages;

    public List<Message> getMessages() {
        return messages;
    }

    public void setMessages(List<Message> messages) {
        this.messages = messages;
    }

    public int getMessage_count() {
        return message_count;
    }

    public void setMessage_count(int message_count) {
        this.message_count = message_count;
    }

    public Map<String,Long> getLeaderLatency() {
        return leaderLatency;
    }

    public void setLeaderLatency(Map<String,Long> leaderLatency) {
        this.leaderLatency = leaderLatency;
    }

    public Map<String,Long> getFollowerLatency() {
        return followerLatency;
    }

    public void setFollowerLatency(Map<String,Long> followerLatency) {
        this.followerLatency = followerLatency;
    }

    @Override
    public String toString() {
        return "ClientResponse{" +
                "result=" + result +
                '}';
    }

    public ClientResponse() {
    }

    public ClientResponse(Object result) {
        this.result = result;
    }

    private ClientResponse(Builder builder) {
        setResult(builder.result);
    }

    public static ClientResponse ok() {
        return new ClientResponse("ok");
    }

    public static ClientResponse fail() {
        return new ClientResponse("fail");
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public int getExtraMessageCount() {
        return extraMessageCount;
    }

    public void setExtraMessageCount(int extraMessageCount) {
        this.extraMessageCount = extraMessageCount;
    }

    public static final class Builder {

        private Object result;

        private Builder() {
        }

        public Builder result(Object val) {
            result = val;
            return this;
        }

        public ClientResponse build() {
            return new ClientResponse(this);
        }
    }
}
