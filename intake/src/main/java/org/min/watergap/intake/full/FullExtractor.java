package org.min.watergap.intake.full;

import org.min.watergap.common.config.BaseConfig;
import org.min.watergap.intake.Extractor;
import org.min.watergap.intake.dialect.DBDialect;
import org.min.watergap.intake.full.rdbms.rs.ResultSetCallback;

import javax.sql.DataSource;
import java.sql.*;

/**
 * 全量导出方法
 *
 * @Create by metaX.h on 2021/11/4 23:26
 */
public abstract class FullExtractor implements Extractor {

    protected DataSource dataSource;

    protected BaseConfig baseConfig;

    protected DBDialect dbDialect;

    public void executeQuery(String querySql, ResultSetCallback resultSetCallback) throws SQLException {
        executeQuery(null, querySql, resultSetCallback);
    }

    public void executeQuery(String catalog, String querySql, ResultSetCallback resultSetCallback) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            if (catalog != null) {
                connection.setCatalog(catalog);
            }
            statement = connection.createStatement();
            resultSet = statement.executeQuery(querySql);
            resultSetCallback.callBack(resultSet);
        } catch (Exception e) {
            throw e;
        } finally {
            releaseConnect(connection, statement, resultSet);
        }
    }

    protected void releaseConnect(Connection connection, Statement statement, ResultSet resultSet) throws SQLException {
        if (connection != null) {
            connection.close();
        }

        if (statement != null) {
            statement.close();
        }

        if (resultSet != null) {
            resultSet.close();
        }
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public BaseConfig getBaseConfig() {
        return baseConfig;
    }

    public void setBaseConfig(BaseConfig baseConfig) {
        this.baseConfig = baseConfig;
    }

    public DBDialect getDbDialect() {
        return dbDialect;
    }

    public void setDbDialect(DBDialect dbDialect) {
        this.dbDialect = dbDialect;
    }
}
