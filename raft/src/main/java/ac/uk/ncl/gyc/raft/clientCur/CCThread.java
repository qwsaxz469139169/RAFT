package ac.uk.ncl.gyc.raft.clientCur;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CCThread extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(CCThread.class);
    private static final UncaughtExceptionHandler uncaughtExceptionHandler = (t, e)
        -> LOGGER.warn("Exception occurred from thread {}", t.getName(), e);

    public CCThread(String threadName, Runnable r) {
        super(r, threadName);
        setUncaughtExceptionHandler(uncaughtExceptionHandler);
    }

}
