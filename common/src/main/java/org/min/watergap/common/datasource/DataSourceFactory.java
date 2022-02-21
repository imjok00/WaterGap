package org.min.watergap.common.datasource;

import org.min.watergap.common.datasource.config.DataSourceConfig;
import org.min.watergap.common.datasource.mysql.MysqlDataSource;

/**
 * 创建合适的数据源
 *
 * @Create by metaX.h on 2022/2/20 21:35
 */
public class DataSourceFactory {

    public static DataSourceWrapper getDataSource(DataSourceConfig config) {
        switch (config.getDatabaseType()) {
            case MYSQL:
                return new MysqlDataSource(config);
        }
        return null;
    }

}
