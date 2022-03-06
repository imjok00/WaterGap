package org.min.watergap.common.datasource.config;

import com.google.gson.JsonObject;
import org.min.watergap.common.config.DatabaseType;
import org.min.watergap.common.datasource.config.mysql.MysqlDataSourceConfig;

/**
 * 数据库配置 - 基础的属性
 *
 * @Create by metaX.h on 2021/11/6 22:38
 */
public abstract class DataSourceConfig {

    /** 配置项 - 起始数据库root配置名 */
    public static final String CONFIG_ROOT_SOURCE = "source";
    /** 配置项 - 目标数据库root配置名 */
    public static final String CONFIG_ROOT_TARGET = "target";
    /** 配置项 - ip */
    public static final String CONFIG_DATASOURCE_IP = "ip";
    /** 配置项 - port */
    public static final String CONFIG_DATASOURCE_PORT = "port";
    /** 配置项 - databaseName */
    public static final String CONFIG_DATASOURCE_DATABASENAME = "databaseName";
    /** 配置项 - user */
    public static final String CONFIG_DATASOURCE_USER = "user";
    /** 配置项 - password */
    public static final String CONFIG_DATASOURCE_PASSWORD = "password";
    /** 配置项 - dbType */
    public static final String CONFIG_DATASOURCE_DBTYPE = "dbType";

    private String ip;

    private String port;

    private String databaseName;

    private String user;

    private String password;

    private boolean readOnly;

    private DatabaseType dbType;

    public abstract String getJdbcUrl();

    public abstract String getDriverClass();

    public String getIp() {
        return ip;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getPort() {
        return port;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public DatabaseType getDatabaseType() {
        return dbType;
    }

    public void load(JsonObject prop){
        this.ip = prop.get(CONFIG_DATASOURCE_IP).getAsString();
        this.port = prop.get(CONFIG_DATASOURCE_PORT).getAsString();
        this.databaseName = prop.get(CONFIG_DATASOURCE_DATABASENAME).getAsString();
        this.user = prop.get(CONFIG_DATASOURCE_USER).getAsString();
        this.password = prop.get(CONFIG_DATASOURCE_PASSWORD).getAsString();
        this.dbType = DatabaseType.valueOf(prop.get(CONFIG_DATASOURCE_DBTYPE).getAsString());
    }

    public static DataSourceConfig getInstance(JsonObject prop) {
        DataSourceConfig dataSourceConfig = null;
        DatabaseType databaseType = DatabaseType.valueOf(prop.get(CONFIG_DATASOURCE_DBTYPE).getAsString());
        switch (databaseType) {
            case MySQL:
                dataSourceConfig = new MysqlDataSourceConfig();
        }
        dataSourceConfig.load(prop);
        return dataSourceConfig;
    }

}
