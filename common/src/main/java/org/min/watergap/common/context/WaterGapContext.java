package org.min.watergap.common.context;

import org.min.watergap.common.config.WaterGapGlobalConfig;
import org.min.watergap.common.config.mode.StartMode;
import org.min.watergap.common.datasource.DataSourceFactory;
import org.min.watergap.common.datasource.DataSourceWrapper;
import org.min.watergap.common.piping.WaterGapPiping;

/**
 * context对象
 */
public class WaterGapContext {

    private static final int MAX_WORKER_NUM = 10000;

    private DataSourceWrapper inDataSource;

    private DataSourceWrapper outDataSource;

    private WaterGapGlobalConfig globalConfig;

    private WaterGapPiping ackPiping;

    private WaterGapPiping pumpPiping;

    public WaterGapContext(WaterGapGlobalConfig globalConfig) {
        this.globalConfig = globalConfig;
        inDataSource = DataSourceFactory.getDataSource(globalConfig.getSourceConfig());
        outDataSource = DataSourceFactory.getDataSource(globalConfig.getTargetConfig());
        ackPiping = new WaterGapPiping();
        pumpPiping = new WaterGapPiping();
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

    public WaterGapPiping getAckPiping() {
        return ackPiping;
    }

    public WaterGapPiping getPumpPiping() {
        return pumpPiping;
    }

    public boolean isIdentical() {
        return globalConfig.getSourceConfig().getDatabaseType()
                .equals(globalConfig.getTargetConfig().getDatabaseType());
    }

    public Long getSqlSelectLimit() {
        return globalConfig.getSqlSelectLimit();
    }

}
