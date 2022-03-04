package org.min.watergap.outfall.rdbms;

import org.min.watergap.common.context.WaterGapContext;
import org.min.watergap.common.datasource.DataSourceWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 关系型数据库，SQL执行器
 */
public class RdbmsDataExecutor implements DataExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(RdbmsDataExecutor.class);

    protected DataSourceWrapper dataSource;

    @Override
    public int execute(String schema, String sql) {
        try {
            return executeUpdate(schema, sql);
        } catch (Exception e) {
            LOG.error("data sql : {} execute error", sql, e);
            return 0;
        }
    }

    private int executeUpdate(String schema, String sql) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = dataSource.getDataSource().getConnection();
            if (schema != null) {
                connection.setCatalog(schema);
            }
            statement = connection.createStatement();
            return statement.executeUpdate(sql);
        } catch (Exception e) {
            throw e;
        } finally {
            releaseConnect(connection, statement);
        }
    }

    protected void releaseConnect(Connection connection, Statement statement) throws SQLException {
        releaseConnect(connection, statement, null);
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

    @Override
    public void init(WaterGapContext waterGapContext) {
        dataSource = waterGapContext.getOutDataSource();
    }

    @Override
    public void destroy() {

    }

    @Override
    public void isStart() {

    }
}
