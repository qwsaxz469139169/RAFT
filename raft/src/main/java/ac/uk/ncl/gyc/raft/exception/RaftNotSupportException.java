package ac.uk.ncl.gyc.raft.exception;

/**
 *
 */
public class RaftNotSupportException extends RuntimeException {

    public RaftNotSupportException() {
    }

    public RaftNotSupportException(String message) {
        super(message);
    }
}
