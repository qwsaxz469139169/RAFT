package ac.uk.ncl.gyc.raft.client;

import java.io.Serializable;

public class ClientRequest implements Serializable {

    public static int PUT = 0;
    public static int GET = 1;

    int type;

    String key;

    String value;

    String sentAdd;

    boolean isRedirect;
    String sentAdd;


    private ClientRequest(Builder builder) {
        setType(builder.type);
        setKey(builder.key);
        setValue(builder.value);
    }

    public String getSentAdd() {
        return sentAdd;
    }

    public void setSentAdd(String sentAdd) {
        this.sentAdd = sentAdd;
    }

    public boolean isRedirect() {
        return isRedirect;
    }

    public void setRedirect(boolean redirect) {
        isRedirect = redirect;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }



    public static Builder newBuilder() {
        return new Builder();
    }

    public enum Type {
        PUT(0), GET(1);
        int code;

        Type(int code) {
            this.code = code;
        }

        public static Type value(int code ) {
            for (Type type : values()) {
                if (type.code == code) {
                    return type;
                }
            }
            return null;
        }
    }


    public static final class Builder {

        private int type;
        private String key;
        private String value;

        private Builder() {
        }


        public Builder type(int val) {
            type = val;
            return this;
        }

        public Builder key(String val) {
            key = val;
            return this;
        }

        public Builder value(String val) {
            value = val;
            return this;
        }

        public ClientRequest build() {
            return new ClientRequest(this);
        }
    }
}
