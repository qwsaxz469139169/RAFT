package ac.uk.ncl.gyc.raft.entity;

import java.io.Serializable;

/**
 * Created by GYC on 2020/7/15.
 */
public class PiggybackingLog implements Serializable {

    private String message;
    private String sentAddr;

    private Long startTime;

    private int extraMessage;

    private boolean firstIndex;

    public String getSentAddr() {
        return sentAddr;
    }

    public void setSentAddr(String sentAddr) {
        this.sentAddr = sentAddr;
    }

    public boolean isFirstIndex() {
        return firstIndex;
    }

    public void setFirstIndex(boolean firstIndex) {
        this.firstIndex = firstIndex;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public int getExtraMessage() {
        return extraMessage;
    }

    public void setExtraMessage(int extraMessage) {
        this.extraMessage = extraMessage;
    }
}
