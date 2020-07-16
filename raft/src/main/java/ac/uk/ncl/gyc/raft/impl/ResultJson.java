package ac.uk.ncl.gyc.raft.impl;

import ac.uk.ncl.gyc.raft.LogModule;
import ac.uk.ncl.gyc.raft.client.Message;
import ac.uk.ncl.gyc.raft.entity.LogEntry;
import com.alibaba.fastjson.JSON;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Created by GYC on 2020/7/16.
 */
public class ResultJson{

    private static final Logger LOGGER = LoggerFactory.getLogger(ResultJson.class);


    /** public just for test */
    public static String dbDir;
    public static String logsDir;

    private static RocksDB logDb;

    public final static byte[] LAST_INDEX_KEY = "LAST_INDEX_KEY".getBytes();

    ReentrantLock lock = new ReentrantLock();

    static {
        if (dbDir == null) {
            dbDir = "./rocksDB-raft-result/" + System.getProperty("serverPort");
        }
        if (logsDir == null) {
            logsDir = dbDir + "/logModule";
        }
        RocksDB.loadLibrary();
    }

    private ResultJson() {
        Options options = new Options();
        options.setCreateIfMissing(true);

        File file = new File(logsDir);
        boolean success = false;
        if (!file.exists()) {
            success = file.mkdirs();
        }
        if (success) {
            LOGGER.warn("make a new dir : " + logsDir);
        }
        try {
            logDb = RocksDB.open(options, logsDir);
        } catch (RocksDBException e) {
            LOGGER.warn(e.getMessage());
        }
    }

    public static ResultJson getInstance() {
        return DefaultLogsLazyHolder.INSTANCE;
    }

    private static class DefaultLogsLazyHolder {

        private static final ResultJson INSTANCE = new ResultJson();
    }


    public void write(Message message) {

        boolean success = false;
        try {

            lock.tryLock(3000, MILLISECONDS);

            logDb.put(message.getMessage().getBytes(), JSON.toJSONBytes(message));
            success = true;
            LOGGER.info("ResultJson write rocksDB success, Message info : [{}]", message);
        } catch (RocksDBException | InterruptedException e) {
            LOGGER.warn(e.getMessage());
        } finally {
            lock.unlock();
        }
    }

    public boolean readAll() {
        RocksIterator iter = logDb.newIterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println("iter key:" + new String(iter.key()) + ", iter value:" + new String(iter.value()));
        }
        return true;
    }


    public Message read(Long index) {
        return null;
    }


    public void removeOnStartIndex(Long startIndex) {

    }


    public LogEntry getLast() {
        return null;
    }


    public Long getLastIndex() {
        return null;
    }
}
