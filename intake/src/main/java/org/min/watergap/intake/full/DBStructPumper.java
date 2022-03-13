package org.min.watergap.intake.full;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.context.WaterGapContext;
import org.min.watergap.common.datasource.DataSourceWrapper;
import org.min.watergap.common.piping.StructPiping;
import org.min.watergap.common.thread.CustomThreadFactory;
import org.min.watergap.intake.Pumper;
import org.min.watergap.intake.dialect.DBDialect;
import org.min.watergap.intake.dialect.DBDialectWrapper;
import org.min.watergap.intake.full.rdbms.result.ResultSetCallback;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.*;

/**
 * 数据结构抽取器
 *
 * @Create by metaX.h on 2021/11/4 23:26
 */
public abstract class DBStructPumper implements Pumper {
    private static final Logger LOG = LogManager.getLogger(DBStructPumper.class);

    private static final int MAX_WORKER_NUM = 10000;

    protected volatile int pumpStatus;

    protected DataSourceWrapper dataSource;

    protected DBDialect pumperDBDialect;

    protected StructPiping structPiping;

    protected StructPiping ackPiping;

    protected WaterGapContext waterGapContext;

    protected Long pollTimeout;

    protected Integer pumpThreadNum;

    protected ThreadPoolExecutor concurrentExecutorWork;

    @Override
    public void init(WaterGapContext waterGapContext) {
        this.waterGapContext = waterGapContext;
        this.dataSource = waterGapContext.getInDataSource();
        this.pumperDBDialect = new DBDialectWrapper(waterGapContext.getGlobalConfig().getSourceConfig().getDatabaseType());
        structPiping = waterGapContext.getStructPiping();
        pollTimeout = waterGapContext.getGlobalConfig().getPollTimeout();
        ackPiping = waterGapContext.getAckPiping();
        pumpThreadNum = waterGapContext.getGlobalConfig().getPumpThreadNum();
        initConcurrentExecutorWork();
    }

    @Override
    public void destroy() {
        try {
            dataSource.revert();
        } catch (IOException e) {
            LOG.info("revert dataSource fail", e);
        }

    }

    public void runPumpWork(Runnable runnable) {
        concurrentExecutorWork.execute(runnable);
    }

    public void executeQuery(String querySql, ResultSetCallback resultSetCallback) throws SQLException, InterruptedException {
        executeQuery(null, querySql, resultSetCallback);
    }

    public void executeQuery(String catalog, String querySql, ResultSetCallback resultSetCallback) throws SQLException, InterruptedException {
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

    public void executeStreamQuery(String catalog, String querySql, ResultSetCallback resultSetCallback) throws SQLException, InterruptedException {
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

    protected void waitStageChange(int semaphoreNum) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(semaphoreNum);

        latch.await();
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

    private void initConcurrentExecutorWork() {
        ArrayBlockingQueue<Runnable> workerQueue = new ArrayBlockingQueue<>(MAX_WORKER_NUM);
        CustomThreadFactory customThreadFactory = new CustomThreadFactory("StructPumper");
        concurrentExecutorWork = new ThreadPoolExecutor(pumpThreadNum, pumpThreadNum,
                0, TimeUnit.MILLISECONDS, workerQueue,
                customThreadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
    }

}
