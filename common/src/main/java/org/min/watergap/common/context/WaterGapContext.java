package org.min.watergap.common.context;

import org.min.watergap.common.config.WaterGapGlobalConfig;
import org.min.watergap.common.config.mode.StartMode;
import org.min.watergap.common.datasource.DataSourceFactory;
import org.min.watergap.common.datasource.DataSourceWrapper;

import java.util.concurrent.CountDownLatch;

/**
 * context对象
 */
public class WaterGapContext {

    private static final int MAX_WORKER_NUM = 10000;

    private DataSourceWrapper inDataSource;

    private DataSourceWrapper outDataSource;

    private WaterGapGlobalConfig globalConfig;

    private CountDownLatch increCnt = new CountDownLatch(1);

    public WaterGapContext(WaterGapGlobalConfig globalConfig) {
        this.globalConfig = globalConfig;
        inDataSource = DataSourceFactory.getDataSource(globalConfig.getSourceConfig());
        outDataSource = DataSourceFactory.getDataSource(globalConfig.getTargetConfig());
    }

    public StartMode getStartMode() {
        return globalConfig.getStarterMode();
    }

    public DataSourceWrapper getInDataSource() {
        return inDataSource;
    }

    public DataSourceWrapper getOutDataSource() {
        return outDataSource;
    }

    public WaterGapGlobalConfig getGlobalConfig() {
        return globalConfig;
    }

    public boolean isIdentical() {
        return globalConfig.getSourceConfig().getDatabaseType()
                .equals(globalConfig.getTargetConfig().getDatabaseType());
    }

    public Long getSqlSelectLimit() {
        return globalConfig.getSqlSelectLimit();
    }

    public CountDownLatch getIncreCnt() {
        return increCnt;
    }
}
