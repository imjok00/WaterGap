package org.min.watergap.intake.full;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.context.WaterGapContext;
import org.min.watergap.common.datasource.DataSourceWrapper;
import org.min.watergap.common.piping.StructPiping;
import org.min.watergap.intake.Pumper;
import org.min.watergap.intake.dialect.DBDialect;
import org.min.watergap.intake.dialect.DBDialectWrapper;
import org.min.watergap.intake.full.rdbms.result.ResultSetCallback;

import java.io.IOException;
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
    private static final Logger LOG = LogManager.getLogger(DBStructPumper.class);

    protected DataSourceWrapper dataSource;

    protected DBDialect pumperDBDialect;

    protected StructPiping structPiping;

    protected WaterGapContext waterGapContext;

    protected Long pollTimeout;

    @Override
    public void init(WaterGapContext waterGapContext) {
        this.waterGapContext = waterGapContext;
        this.dataSource = waterGapContext.getInDataSource();
        this.pumperDBDialect = new DBDialectWrapper(waterGapContext.getGlobalConfig().getSourceConfig().getDatabaseType());
        structPiping = waterGapContext.getStructPiping();
        pollTimeout = waterGapContext.getGlobalConfig().getPollTimeout();
    }

    @Override
    public void destroy() {
        try {
            dataSource.revert();
        } catch (IOException e) {
            LOG.info("revert dataSource fail", e);
        }

    }

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