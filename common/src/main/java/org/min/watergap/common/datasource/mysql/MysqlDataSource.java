package org.min.watergap.common.datasource.mysql;

import org.min.watergap.common.datasource.AbstractDataSourceWrapper;
import org.min.watergap.common.datasource.config.DataSourceConfig;

import java.io.Closeable;
import java.io.IOException;

/**
 * Mysql数据源
 *
 * @Create by metaX.h on 2021/11/4 23:33
 */
public class MysqlDataSource extends AbstractDataSourceWrapper {

    public MysqlDataSource(DataSourceConfig dataSourceConfig) {
        super(dataSourceConfig);
    }


    public void revert() throws IOException {
        ((Closeable)getDataSource()).close();
    }
}
