package org.min.watergap.common.local.storage.datasource;

import org.min.watergap.common.datasource.AbstractDataSourceWrapper;
import org.min.watergap.common.datasource.config.DataSourceConfig;

import java.io.IOException;

/**
 * TODO:
 *
 * @Create by metaX.h on 2021/11/8 23:11
 */
public class LocalStorageSource extends AbstractDataSourceWrapper {
    public LocalStorageSource(DataSourceConfig dataSourceConfig) {
        super(dataSourceConfig);
    }

    @Override
    public void revert() throws IOException {

    }
}
