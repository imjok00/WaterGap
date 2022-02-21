package org.min.watergap.common.context;

import org.min.watergap.common.config.WaterGapGlobalConfig;
import org.min.watergap.common.datasource.DataSourceWrapper;

public class WaterGapContext {

    private DataSourceWrapper inDataSource;

    private DataSourceWrapper outDataSource;

    private WaterGapGlobalConfig globalConfig;

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
}
