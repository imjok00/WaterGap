package org.min.watergap.common.context;

import org.min.watergap.common.config.WaterGapGlobalConfig;
import org.min.watergap.common.config.mode.StartMode;
import org.min.watergap.common.datasource.DataSourceFactory;
import org.min.watergap.common.datasource.DataSourceWrapper;
import org.min.watergap.common.piping.StructPiping;

/**
 * context对象
 */
public class WaterGapContext {

    private DataSourceWrapper inDataSource;

    private DataSourceWrapper outDataSource;

    private WaterGapGlobalConfig globalConfig;

    private StructPiping structPiping;

    public WaterGapContext(WaterGapGlobalConfig globalConfig) {
        this.globalConfig = globalConfig;
        inDataSource = DataSourceFactory.getDataSource(globalConfig.getSourceConfig());
        outDataSource = DataSourceFactory.getDataSource(globalConfig.getTargetConfig());
        structPiping = new StructPiping();
    }

    public StartMode getStartMode() {
        return globalConfig.getStarterMode();
    }

    public DataSourceWrapper getInDataSource() {
        return inDataSource;
    }

    public void setInDataSource(DataSourceWrapper inDataSource) {
        this.inDataSource = inDataSource;
    }

    public DataSourceWrapper getOutDataSource() {
        return outDataSource;
    }

    public void setOutDataSource(DataSourceWrapper outDataSource) {
        this.outDataSource = outDataSource;
    }

    public WaterGapGlobalConfig getGlobalConfig() {
        return globalConfig;
    }

    public void setGlobalConfig(WaterGapGlobalConfig globalConfig) {
        this.globalConfig = globalConfig;
    }

    public StructPiping getStructPiping() {
        return structPiping;
    }

    public void setStructPiping(StructPiping structPiping) {
        this.structPiping = structPiping;
    }
}
