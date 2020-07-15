package ac.uk.ncl.gyc.raft.entity;

import java.util.List;

/**
 * Created by GYC on 2020/6/27.
 */
public class CommitRequest extends BaseRequest{

    String message;

    List<LogEntry> logEntries;

    public List<LogEntry> getLogEntries() {
        return logEntries;
    }

    public void setLogEntries(List<LogEntry> logEntry) {
        this.logEntries = logEntries;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
