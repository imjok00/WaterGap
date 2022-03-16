package org.min.watergap.common.context;

import org.min.watergap.common.config.WaterGapGlobalConfig;
import org.min.watergap.common.config.mode.StartMode;
import org.min.watergap.common.datasource.DataSourceFactory;
import org.min.watergap.common.datasource.DataSourceWrapper;
import org.min.watergap.common.piping.StructPiping;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * context对象
 */
public class WaterGapContext {

    private static final int MAX_WORKER_NUM = 10000;

    private DataSourceWrapper inDataSource;

    private DataSourceWrapper outDataSource;

    private WaterGapGlobalConfig globalConfig;

    private StructPiping structPiping;

    private StructPiping ackPiping;

    private ThreadPoolExecutor concurrentExecutorWork;

    public WaterGapContext(WaterGapGlobalConfig globalConfig) {
        this.globalConfig = globalConfig;
        inDataSource = DataSourceFactory.getDataSource(globalConfig.getSourceConfig());
        outDataSource = DataSourceFactory.getDataSource(globalConfig.getTargetConfig());
        structPiping = new StructPiping();
        ackPiping = new StructPiping();
        initConcurrentExecutorWork(globalConfig.getExecutorWorkNum());
    }

    private void initConcurrentExecutorWork(int pumpThreadNum) {
        ArrayBlockingQueue<Runnable> workerQueue = new ArrayBlockingQueue<>(MAX_WORKER_NUM);
        concurrentExecutorWork = new ThreadPoolExecutor(pumpThreadNum, pumpThreadNum,
                50, TimeUnit.MILLISECONDS, workerQueue,
                Executors.defaultThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public StartMode getStartMode() {
        return globalConfig.getStarterMode();
    }

    public DataSourceWrapper getInDataSource() {
        return inDataSource;
    }

    public DataSourceWrapper getOutDataSource() {
        return outDataSource;
    }

    public WaterGapGlobalConfig getGlobalConfig() {
        return globalConfig;
    }

    public StructPiping getStructPiping() {
        return structPiping;
    }

    public StructPiping getAckPiping() {
        return ackPiping;
    }

    public ThreadPoolExecutor getConcurrentExecutorWork() {
        return concurrentExecutorWork;
    }
}
