package org.min.watergap.outfall;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.config.DatabaseType;
import org.min.watergap.common.context.WaterGapContext;
import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.common.lifecycle.AbstractWaterGapLifeCycle;
import org.min.watergap.outfall.rdbms.DataExecutor;
import org.min.watergap.outfall.rdbms.RdbmsDataExecutor;
import org.min.watergap.piping.thread.SingleThreadWorkGroup;
import org.min.watergap.piping.translator.PipingData;
import org.min.watergap.piping.translator.WaterGapPiping;

public abstract class OutFallDrainer extends AbstractWaterGapLifeCycle implements Drainer {
    private static final Logger LOG = LogManager.getLogger(OutFallDrainer.class);

    protected WaterGapContext waterGapContext;

    protected DataExecutor dataExecutor;

    protected DatabaseType targetDBType;

    protected WaterGapPiping ackPiping;

    protected WaterGapPiping pumpPiping;

    protected SingleThreadWorkGroup[] singleThreadWorkGroups;

    protected abstract void doExecute(PipingData dataStruct);


    @Override
    public void apply() {
        start();
        startRunDrainer();
    }

    private void startRunDrainer() {
        for (int i = 0; i < singleThreadWorkGroups.length; i++) {
            singleThreadWorkGroups[i] = new SingleThreadWorkGroup(pipingData -> {
                doExecute(pipingData);
                return 1;
            }, pumpPiping);
            singleThreadWorkGroups[i].init(waterGapContext);
            singleThreadWorkGroups[i].start();
        }
    }

    protected void ack(PipingData pipingData) throws WaterGapException {
        try {
            ackPiping.put(pipingData);
        } catch (InterruptedException e) {
            throw new WaterGapException("ack data error", e);
        }
    }

    public void injectPiping(WaterGapPiping pumpPiping, WaterGapPiping ackPiping) {
        this.pumpPiping = pumpPiping;
        this.ackPiping = ackPiping;
    }


    @Override
    public void init(WaterGapContext waterGapContext) {
        this.waterGapContext = waterGapContext;
        dataExecutor = new RdbmsDataExecutor();
        ((AbstractWaterGapLifeCycle)dataExecutor).init(waterGapContext);
        targetDBType = waterGapContext.getGlobalConfig().getTargetConfig().getDatabaseType();
        singleThreadWorkGroups = new SingleThreadWorkGroup[waterGapContext.getGlobalConfig().getExecutorWorkNum()];
    }

    @Override
    public void destroy() {
        if (singleThreadWorkGroups != null) {
            for (SingleThreadWorkGroup singleThreadWorkGroup : singleThreadWorkGroups) {
                singleThreadWorkGroup.destroy();
            }
        }
    }
}