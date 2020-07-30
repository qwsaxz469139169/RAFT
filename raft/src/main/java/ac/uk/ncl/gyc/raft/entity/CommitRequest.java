package ac.uk.ncl.gyc.raft.entity;

import java.util.List;
import java.util.Map;

/**
 * Created by GYC on 2020/6/27.
 */
public class CommitRequest extends BaseRequest{

    String message;

    List<LogEntry> logEntries;

    Map<String,String> newMap;

    public Map<String, String> getNewMap() {
        return newMap;
    }

    public void setNewMap(Map<String, String> newMap) {
        this.newMap = newMap;
    }

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
