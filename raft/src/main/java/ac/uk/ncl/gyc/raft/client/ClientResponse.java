package ac.uk.ncl.gyc.raft.client;

import java.io.Serializable;
import java.util.List;

public class ClientResponse implements Serializable {

    Object result;

    int extraMessageCount;

    long leaderLatency;

    long followerLatency;

    List<String> requests;

    public List<String> getRequests() {
        return requests;
    }

    public void setRequests(List<String> requests) {
        this.requests = requests;
    }

    public long getLeaderLatency() {
        return leaderLatency;
    }

    public void setLeaderLatency(long leaderLatency) {
        this.leaderLatency = leaderLatency;
    }

    public long getFollowerLatency() {
        return followerLatency;
    }

    public void setFollowerLatency(long followerLatency) {
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
