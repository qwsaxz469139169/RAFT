package ac.uk.ncl.gyc.raft;

import ac.uk.ncl.gyc.raft.entity.LogEntry;

/**
 *
 * @see LogEntry
 * @author Yuchen Guo
 */
public interface LogModule {

    void write(LogEntry logEntry);

    LogEntry read(Long index);

    void removeOnStartIndex(Long startIndex);

    LogEntry getLast();

    Long getLastIndex();
}
