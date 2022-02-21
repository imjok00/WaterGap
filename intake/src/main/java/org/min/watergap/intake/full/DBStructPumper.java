package org.min.watergap.intake.full;

import org.min.watergap.common.config.BaseConfig;
import org.min.watergap.common.datasource.DataSourceWrapper;
import org.min.watergap.intake.Pumper;
import org.min.watergap.intake.dialect.DBDialect;
import org.min.watergap.intake.full.rdbms.result.ResultSetCallback;
import org.min.watergap.piping.pip.StructPiping;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 数据结构抽取器
 *
 * @Create by metaX.h on 2021/11/4 23:26
 */
public abstract class DBStructPumper implements Pumper {

    protected DataSourceWrapper dataSource;

    protected BaseConfig baseConfig;

    protected DBDialect pumperDBDialect;

    protected StructPiping fullPiping;




    public void executeQuery(String querySql, ResultSetCallback resultSetCallback) throws SQLException {
        executeQuery(null, querySql, resultSetCallback);
    }

    public void executeQuery(String catalog, String querySql, ResultSetCallback resultSetCallback) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getDataSource().getConnection();
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

    public void executeStreamQuery(String catalog, String querySql, ResultSetCallback resultSetCallback) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getDataSource().getConnection();
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

}
