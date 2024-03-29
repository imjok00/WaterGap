package org.min.watergap.outfall.rdbms;

import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.piping.translator.PipingData;
import org.min.watergap.piping.translator.impl.FullTableDataPipingData;

import java.util.List;

public interface DataExecutor {

    int execute(String schema, String sql, PipDataAck callback);

    int executeBatch(String schema, String sql, FullTableDataPipingData.ColumnValContain contain, PipDataAck callback) throws WaterGapException;

    int executeIncDataWithTrans(String schema, List<PipingData> transactionBuffer);
}