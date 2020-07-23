package ac.uk.ncl.gyc.raft.common;

import java.util.Objects;

public class PeerNode {

    /**
     * ip:selfPort
     */
    private final String address;


    public PeerNode(String addr) {
        this.address = addr;
    }

    public String getAdress() {
        return address;

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PeerNode peer = (PeerNode) o;
        return Objects.equals(address, peer.address);
    }

    @Override
    public int hashCode() {

        return Objects.hash(address);
    }

    @Override
    public String toString() {
        return "PeerNode{" +
                "addr='" + address + '\'' +
                '}';
    }
}
