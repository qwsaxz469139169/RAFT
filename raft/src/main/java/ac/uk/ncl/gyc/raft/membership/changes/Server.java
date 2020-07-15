package ac.uk.ncl.gyc.raft.membership.changes;

import lombok.Getter;
import lombok.Setter;

public class Server {

    String address;


    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}
