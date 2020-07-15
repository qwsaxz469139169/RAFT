package ac.uk.ncl.gyc.raft.entity;

/**
 * Created by GYC on 2020/6/27.
 */
public class CommitRequest extends BaseRequest{

    String message;

    LogEntry logEntry;

    public LogEntry getLogEntry() {
        return logEntry;
    }

    public void setLogEntry(LogEntry logEntry) {
        this.logEntry = logEntry;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
