package org.min.watergap.common.datasource.config;

/**
 * 数据库配置 - 基础的属性
 *
 * @Create by metaX.h on 2021/11/6 22:38
 */
public abstract class DataSourceConfig {

    private String ip;

    private String port;

    private String databaseName;

    private String user;

    private String password;

    private boolean readOnly;

    public abstract String getJdbcUrl();

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }
}
