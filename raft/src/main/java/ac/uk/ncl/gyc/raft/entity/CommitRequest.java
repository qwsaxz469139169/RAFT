package ac.uk.ncl.gyc.raft.entity;

import java.util.List;

/**
 * Created by GYC on 2020/6/27.
 */
public class CommitRequest extends BaseRequest {

    String message;

    List<String> messages;

    public List<String> getMessages() {
        return messages;
    }

    public void setMessages(List<String> messages) {
        this.messages = messages;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
