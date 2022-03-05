package org.min.watergap.intake.full;

import org.min.watergap.common.config.BaseConfig;
import org.min.watergap.intake.Pumper;
import org.min.watergap.intake.dialect.DBDialect;
import org.min.watergap.intake.full.rdbms.result.ResultSetCallback;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 数据内容抽取器
 *
 * @Create by metaX.h on 2021/11/4 23:26
 */
public abstract class DBDataPumper implements Pumper {

    protected DataSource dataSource;

    protected BaseConfig baseConfig;

    protected DBDialect sourceDBDialect;



    public void executeQuery(String querySql, ResultSetCallback resultSetCallback) throws SQLException, InterruptedException {
        executeQuery(null, querySql, resultSetCallback);
    }

    public void executeQuery(String catalog, String querySql, ResultSetCallback resultSetCallback) throws SQLException, InterruptedException {
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

    public void executeStreamQuery(String catalog, String querySql, ResultSetCallback resultSetCallback) throws SQLException, InterruptedException {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            if (catalog != null) {
                connection.setCatalog(catalog);
            }
            statement = connection.prepareStatement(querySql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            statement.setFetchSize(Integer.MIN_VALUE);
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
        return sourceDBDialect;
    }

    public void setDbDialect(DBDialect dbDialect) {
        this.sourceDBDialect = dbDialect;
    }
}
