package ac.uk.ncl.gyc.raft.entity;

import ac.uk.ncl.gyc.raft.Consensus;
import lombok.Getter;
import lombok.Setter;

/**
 * 请求投票 RPC 参数.
 *
 * @author Yuchen Guo
 * @see Consensus#requestVote(ElectionTaskRequest)
 */
@Getter
@Setter
public class ElectionTaskRequest extends BaseRequest {

    /**
     * 请求选票的候选人的 Id(ip:selfPort)
     */
    String candidateId;

    /**
     * 候选人的最后日志条目的索引值
     */
    long lastLogIndex;

    /**
     * 候选人最后日志条目的任期号
     */
    long lastLogTerm;

    private ElectionTaskRequest(Builder builder) {
        setTerm(builder.term);
        setServerId(builder.serverId);
        setCandidateId(builder.candidateId);
        setLastLogIndex(builder.lastLogIndex);
        setLastLogTerm(builder.lastLogTerm);
    }

    public String getCandidateId() {
        return candidateId;
    }

    public void setCandidateId(String candidateId) {
        this.candidateId = candidateId;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public void setLastLogIndex(long lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
    }

    public long getLastLogTerm() {
        return lastLogTerm;
    }

    public void setLastLogTerm(long lastLogTerm) {
        this.lastLogTerm = lastLogTerm;
    }

    @Override
    public String toString() {
        return "ElectionTaskRequest{" +
                "candidateId='" + candidateId + '\'' +
                ", lastLogIndex=" + lastLogIndex +
                ", lastLogTerm=" + lastLogTerm +
                ", term=" + term +
                ", serverId='" + serverId + '\'' +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }


    public static final class Builder {

        private long term;
        private String serverId;
        private String candidateId;
        private long lastLogIndex;
        private long lastLogTerm;

        private Builder() {
        }

        public Builder term(long val) {
            term = val;
            return this;
        }

        public Builder serverId(String val) {
            serverId = val;
            return this;
        }

        public Builder candidateId(String val) {
            candidateId = val;
            return this;
        }

        public Builder lastLogIndex(long val) {
            lastLogIndex = val;
            return this;
        }

        public Builder lastLogTerm(long val) {
            lastLogTerm = val;
            return this;
        }

        public ElectionTaskRequest build() {
            return new ElectionTaskRequest(this);
        }
    }
}
