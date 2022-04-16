package org.min.watergap.outfall.rdbms;

import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.piping.translator.impl.FullTableDataBasePipingData;

public interface DataExecutor {

    int execute(String schema, String sql, PipDataAck callback);

    int executeBatch(String schema, String sql, FullTableDataBasePipingData.ColumnValContain contain, PipDataAck callback) throws WaterGapException;
}