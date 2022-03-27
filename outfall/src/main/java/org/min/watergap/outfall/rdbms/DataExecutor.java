package org.min.watergap.outfall.rdbms;

import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.common.lifecycle.WaterGapLifeCycle;
import org.min.watergap.common.piping.data.impl.FullTableDataBasePipingData;

public interface DataExecutor extends WaterGapLifeCycle {

    int execute(String schema, String sql, PipDataAck callback);

    int executeBatch(String schema, String sql, FullTableDataBasePipingData.ColumnValContain contain, PipDataAck callback) throws WaterGapException;
}