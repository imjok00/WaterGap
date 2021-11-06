package org.min.watergap.common.datasource.config.mysql;

import org.min.watergap.common.datasource.config.DataSourceConfig;

/**
 * TODO:
 *
 * @Create by metaX.h on 2021/11/6 22:48
 */
public class MysqlDataSourceConfig extends DataSourceConfig {

    private static final String JDBC_URL_PREFIX = "jdbc:mysql://";
    private static final String JDBC_URL_SEMI = ":";
    private static final String JDBC_URL_BSLASH = "/";

    public String getJdbcUrl() {
        return JDBC_URL_PREFIX + getIp() + JDBC_URL_SEMI + getPort() + JDBC_URL_BSLASH + getDatabaseName();
    }


}
