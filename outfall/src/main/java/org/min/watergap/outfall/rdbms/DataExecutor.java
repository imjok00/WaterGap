package org.min.watergap.outfall.rdbms;

import org.min.watergap.common.lifecycle.WaterGapLifeCycle;

public interface DataExecutor extends WaterGapLifeCycle {

    int execute(String schema, String sql);

}