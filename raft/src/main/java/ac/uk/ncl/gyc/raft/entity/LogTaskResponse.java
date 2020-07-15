package ac.uk.ncl.gyc.raft.entity;

import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 *
 * 附加 RPC 日志返回值.
 *
 * @author Yuchen Guo
 */

public class LogTaskResponse implements Serializable {

    /** 当前的任期号，用于领导人去更新自己 */
    long term;

    /** 跟随者包含了匹配上 prevLogIndex 和 prevLogTerm 的日志时为真  */
    boolean success;

    public LogTaskResponse(long term) {
        this.term = term;
    }

    public LogTaskResponse(boolean success) {
        this.success = success;
    }

    public LogTaskResponse(long term, boolean success) {
        this.term = term;
        this.success = success;
    }

    private LogTaskResponse(Builder builder) {
        setTerm(builder.term);
        setSuccess(builder.success);
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public static LogTaskResponse fail() {
        return new LogTaskResponse(false);
    }

    public static LogTaskResponse ok() {
        return new LogTaskResponse(true);
    }

    public static Builder newBuilder() {
        return new Builder();
    }


    public static final class Builder {

        private long term;
        private boolean success;

        private Builder() {
        }

        public Builder term(long val) {
            term = val;
            return this;
        }

        public Builder success(boolean val) {
            success = val;
            return this;
        }

        public LogTaskResponse build() {
            return new LogTaskResponse(this);
        }
    }
}
