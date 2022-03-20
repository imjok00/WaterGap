package org.min.watergap.outfall;

import org.min.watergap.common.config.DatabaseType;
import org.min.watergap.common.context.WaterGapContext;
import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.common.piping.PipingData;
import org.min.watergap.common.piping.WaterGapPiping;
import org.min.watergap.common.piping.struct.impl.BasePipingData;
import org.min.watergap.outfall.rdbms.DataExecutor;
import org.min.watergap.outfall.rdbms.RdbmsDataExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadPoolExecutor;

public abstract class OutFallDrainer implements Drainer {
    private static final Logger LOG = LoggerFactory.getLogger(OutFallDrainer.class);

    protected DataExecutor dataExecutor;

    protected WaterGapPiping structPiping;

    protected DatabaseType targetDBType;

    protected WaterGapPiping ackPiping;

    protected ThreadPoolExecutor concurrentExecutorWork;

    protected abstract void doExecute(BasePipingData dataStruct);

    @Override
    public void destroy() {
        dataExecutor.destroy();
    }

    @Override
    public void isStart() {

    }

    @Override
    public void apply() {
        try {
            for (;;) {
                PipingData pipingData = structPiping.take();
                concurrentExecutorWork.execute(() -> {
                    doExecute((BasePipingData) pipingData);
                });
            }
        } catch (InterruptedException e) {
            LOG.error("poll event from fsink fail", e);
        }
    }

    protected void ack(BasePipingData pipingData) throws WaterGapException {
        try {
            ackPiping.put(pipingData);
        } catch (InterruptedException e) {
            throw new WaterGapException("ack data error", e);
        }
    }


    @Override
    public void init(WaterGapContext waterGapContext) {
        dataExecutor = new RdbmsDataExecutor();
        dataExecutor.init(waterGapContext);
        structPiping = waterGapContext.getStructPiping();
        targetDBType = waterGapContext.getGlobalConfig().getTargetConfig().getDatabaseType();
        ackPiping = waterGapContext.getAckPiping();
        concurrentExecutorWork = waterGapContext.getConcurrentExecutorWork();
    }
}