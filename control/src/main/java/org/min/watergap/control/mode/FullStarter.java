package org.min.watergap.control.mode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.context.WaterGapContext;
import org.min.watergap.common.lifecycle.AbstractWaterGapLifeCycle;
import org.min.watergap.common.lifecycle.WaterGapLifeCycle;
import org.min.watergap.intake.Pumper;
import org.min.watergap.intake.full.rdbms.RdbmsDataPumper;
import org.min.watergap.intake.full.rdbms.extractor.MysqlDBAllTypePumper;
import org.min.watergap.outfall.Drainer;
import org.min.watergap.outfall.OutFallDrainer;
import org.min.watergap.outfall.full.RdbmsOutFallDrainer;
import org.min.watergap.piping.translator.WaterGapPiping;

/**
 * 全量模式启动 <br/>
 * 只迁移全量数据
 *
 * @Create by metaX.h on 2021/11/3 23:52
 */
public class FullStarter extends AbstractWaterGapLifeCycle implements Runner {
    private static final Logger LOG = LogManager.getLogger(FullStarter.class);

    private Pumper fullPumper;

    private WaterGapPiping pumpPiping;

    private Drainer fullDrainer;

    private WaterGapPiping ackPiping;

    private WaterGapContext waterGapContext;

    @Override
    public void init(WaterGapContext waterGapContext) {
        this.waterGapContext = waterGapContext;
        switch (waterGapContext.getGlobalConfig().getSourceConfig().getDatabaseType()) {
            case MySQL:
                LOG.info("# Init MySQL pumper...");
                fullPumper = new MysqlDBAllTypePumper();
                break;
        }
        switch (waterGapContext.getGlobalConfig().getTargetConfig().getDatabaseType()) {
            case MySQL:
                LOG.info("# Init MySQL drainer...");
                fullDrainer = new RdbmsOutFallDrainer();
                break;
        }
        pumpPiping = new WaterGapPiping();
        ackPiping = new WaterGapPiping();

        ((RdbmsDataPumper)fullPumper).injectPiping(pumpPiping, ackPiping);
        ((OutFallDrainer)fullDrainer).injectPiping(pumpPiping, ackPiping);
        ((WaterGapLifeCycle)fullPumper).init(waterGapContext);
        ((WaterGapLifeCycle)fullDrainer).init(waterGapContext);
    }

    @Override
    public void start() {
        super.start();
        LOG.info("### Full Pumper Starter ###");
        fullPumper.pump();
        LOG.info("### Full Drainer Starter ###");
        fullDrainer.apply();
    }

    @Override
    public void waitForShutdown() throws InterruptedException {
        waterGapContext.getFullCntLatch().await();
    }

    @Override
    public void destroy() {
        LOG.info("ready to destroy");
        super.stop();
        ((WaterGapLifeCycle) fullPumper).destroy();
        ((WaterGapLifeCycle) fullDrainer).destroy();
    }

}
