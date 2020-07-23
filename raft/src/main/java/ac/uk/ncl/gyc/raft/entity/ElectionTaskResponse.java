package ac.uk.ncl.gyc.raft.entity;

import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

/**
 * 请求投票 RPC 返回值.
 */
public class ElectionTaskResponse implements Serializable {

    /**
     * 当前任期号，以便于候选人去更新自己的任期
     */
    long term;

    /**
     * 候选人赢得了此张选票时为真
     */
    boolean voteGranted;

    public ElectionTaskResponse(boolean voteGranted) {
        this.voteGranted = voteGranted;
    }

    private ElectionTaskResponse(Builder builder) {
        setTerm(builder.term);
        setVoteGranted(builder.voteGranted);
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    public void setVoteGranted(boolean voteGranted) {
        this.voteGranted = voteGranted;
    }

    public static ElectionTaskResponse fail() {
        return new ElectionTaskResponse(false);
    }

    public static ElectionTaskResponse ok() {
        return new ElectionTaskResponse(true);
    }

    public static Builder newBuilder() {
        return new Builder();
    }


    public static final class Builder {

        private long term;
        private boolean voteGranted;

        private Builder() {
        }

        public Builder term(long term) {
            this.term = term;
            return this;
        }

        public Builder voteGranted(boolean voteGranted) {
            this.voteGranted = voteGranted;
            return this;
        }

        public ElectionTaskResponse build() {
            return new ElectionTaskResponse(this);
        }
    }
}
