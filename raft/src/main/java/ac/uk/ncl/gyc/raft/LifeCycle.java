package ac.uk.ncl.gyc.raft;

/**
 * @author Yuchen Guo
 */
public interface LifeCycle {

    void init() throws Throwable;

    void destroy() throws Throwable;
}
