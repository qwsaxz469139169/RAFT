package ac.uk.ncl.gyc.raft.rpc;

import java.io.Serializable;

import ac.uk.ncl.gyc.raft.entity.ElectionTaskRequest;
import ac.uk.ncl.gyc.raft.entity.LogTaskRequest;
import ac.uk.ncl.gyc.raft.client.ClientRequest;

public class Request<T> implements Serializable {

    /**
     * 请求投票
     */
    public static final int REQ_VOTE = 0;
    /**
     * 附加日志
     */
    public static final int REQ_LOG = 1;
    /**
     * 客户端
     */
    public static final int REQ_CLIENT = 2;
    /**
     * 配置变更. add
     */
    public static final int CHANGE_CONFIG_ADD = 3;
    /**
     * 配置变更. remove
     */
    public static final int CHANGE_CONFIG_REMOVE = 4;

    public static final int REQ_COMMIT = 5;
    /**
     * 请求类型
     */
    private int cmd = -1;

    /**
     * param
     *
     * @see LogTaskRequest
     * @see ElectionTaskRequest
     * @see ClientRequest
     */
    private T obj;

    String url;

    public Request() {
    }

    public Request(T obj) {
        this.obj = obj;
    }

    public Request(int cmd, T obj, String url) {
        this.cmd = cmd;
        this.obj = obj;
        this.url = url;
    }

    private Request(Builder builder) {
        setCmd(builder.cmd);
        setObj((T) builder.obj);
        setUrl(builder.url);
    }

    public int getCmd() {
        return cmd;
    }

    public void setCmd(int cmd) {
        this.cmd = cmd;
    }

    public T getObj() {
        return obj;
    }

    public void setObj(T obj) {
        this.obj = obj;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public static Builder newBuilder() {
        return new Builder<>();
    }


    public final static class Builder<T> {

        private int cmd;
        private Object obj;
        private String url;

        private Builder() {
        }

        public Builder cmd(int val) {
            cmd = val;
            return this;
        }

        public Builder obj(Object val) {
            obj = val;
            return this;
        }

        public Builder url(String val) {
            url = val;
            return this;
        }

        public Request<T> build() {
            return new Request<T>(this);
        }
    }

}
