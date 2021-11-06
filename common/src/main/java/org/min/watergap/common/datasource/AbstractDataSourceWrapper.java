package org.min.watergap.common.datasource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.min.watergap.common.datasource.config.DataSourceConfig;

import javax.sql.DataSource;

/**
 * TODO:
 *
 * @Create by metaX.h on 2021/11/4 23:33
 */
public abstract class AbstractDataSourceWrapper implements DataSourceWrapper {

    // CPU 核心数量
    public static int CORE_PROCESSOR_COUNT = Runtime.getRuntime().availableProcessors();

    private DataSource dataSource;

    private DataSourceConfig dataSourceConfig;

    public AbstractDataSourceWrapper(DataSourceConfig dataSourceConfig) {
        this.dataSourceConfig = dataSourceConfig;
        createDataSource();
    }

    private void createDataSource() {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(dataSourceConfig.getJdbcUrl());
        hikariConfig.setUsername(dataSourceConfig.getUser());
        hikariConfig.setPassword(dataSourceConfig.getPassword());
        hikariConfig.setReadOnly(dataSourceConfig.isReadOnly());
        hikariConfig.setMaximumPoolSize(CORE_PROCESSOR_COUNT);

        this.dataSource = new HikariDataSource(hikariConfig);
    }

    public DataSource getDataSource() {
        if (dataSource != null) {
            return dataSource;
        }
        return null;
    }

}
