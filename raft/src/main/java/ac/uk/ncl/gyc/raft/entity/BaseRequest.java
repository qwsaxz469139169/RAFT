package ac.uk.ncl.gyc.raft.entity;

import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 *
 */
public class BaseRequest implements Serializable {

    /**
     * 候选人的任期号
     */
    public long term;

    /**
     * 被请求者 ID(ip:selfPort)
     */
    public String serverId;

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    @Override
    public String toString() {
        return "BaseRequest{" +
                "term=" + term +
                ", serverId='" + serverId + '\'' +
                '}';
    }
}
