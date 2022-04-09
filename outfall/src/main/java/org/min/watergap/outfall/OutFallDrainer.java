package org.min.watergap.outfall;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.config.DatabaseType;
import org.min.watergap.common.context.WaterGapContext;
import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.common.lifecycle.AbstractWaterGapLifeCycle;
import org.min.watergap.common.local.storage.orm.service.FullTableDataPositionService;
import org.min.watergap.common.local.storage.orm.service.FullTableStatusService;
import org.min.watergap.common.local.storage.orm.service.MigrateStageService;
import org.min.watergap.common.local.storage.orm.service.SchemaStatusService;
import org.min.watergap.common.piping.PipingData;
import org.min.watergap.common.piping.WaterGapPiping;
import org.min.watergap.outfall.rdbms.DataExecutor;
import org.min.watergap.outfall.rdbms.RdbmsDataExecutor;
import org.min.watergap.piping.thread.SingleThreadWorkGroup;

public abstract class OutFallDrainer extends AbstractWaterGapLifeCycle implements Drainer {
    private static final Logger LOG = LogManager.getLogger(OutFallDrainer.class);

    protected WaterGapContext waterGapContext;

    protected DataExecutor dataExecutor;

    protected DatabaseType targetDBType;

    protected WaterGapPiping ackPiping;

    protected FullTableStatusService fullTableStatusService;

    protected FullTableDataPositionService fullTableDataPositionService;

    protected MigrateStageService migrateStageService;

    protected SchemaStatusService schemaStatusService;

    protected SingleThreadWorkGroup[] singleThreadWorkGroups;

    protected abstract void doExecute(PipingData dataStruct);


    @Override
    public void apply() {
        start();
        startRunDrainer();
    }

    private void startRunDrainer() {
        for (SingleThreadWorkGroup singleThreadWorkGroup : singleThreadWorkGroups) {
            singleThreadWorkGroup = new SingleThreadWorkGroup(pipingData -> {
                doExecute(pipingData);
                return 1;
            }, waterGapContext.getPumpPiping());
            singleThreadWorkGroup.start();
        }
    }

    protected void ack(PipingData pipingData) throws WaterGapException {
        try {
            ackPiping.put(pipingData);
        } catch (InterruptedException e) {
            throw new WaterGapException("ack data error", e);
        }
    }


    @Override
    public void init(WaterGapContext waterGapContext) {
        this.waterGapContext = waterGapContext;
        dataExecutor = new RdbmsDataExecutor();
        ((AbstractWaterGapLifeCycle)dataExecutor).init(waterGapContext);
        targetDBType = waterGapContext.getGlobalConfig().getTargetConfig().getDatabaseType();
        ackPiping = waterGapContext.getAckPiping();
        singleThreadWorkGroups = new SingleThreadWorkGroup[waterGapContext.getGlobalConfig().getExecutorWorkNum()];
        fullTableStatusService = new FullTableStatusService();
        migrateStageService = new MigrateStageService();
        schemaStatusService = new SchemaStatusService();
        fullTableDataPositionService = new FullTableDataPositionService();
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