package org.min.watergap.control.mode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.context.WaterGapContext;
import org.min.watergap.common.lifecycle.AbstractWaterGapLifeCycle;
import org.min.watergap.common.lifecycle.WaterGapLifeCycle;
import org.min.watergap.intake.Pumper;
import org.min.watergap.intake.full.rdbms.RdbmsDataPumper;
import org.min.watergap.intake.full.rdbms.extractor.MysqlDBAllTypePumper;
import org.min.watergap.intake.incre.IncreLogPumper;
import org.min.watergap.intake.incre.rdbms.MysqlIncreLogPumper;
import org.min.watergap.outfall.Drainer;
import org.min.watergap.outfall.OutFallDrainer;
import org.min.watergap.outfall.full.RdbmsOutFallDrainer;
import org.min.watergap.piping.translator.WaterGapPiping;

/**
 * 全量+增量的方式 <br/>
 * 进行迁移
 *
 * @Create by metaX.h on 2021/11/3 23:52
 */
public class FullAndIncreStarter extends AbstractWaterGapLifeCycle implements Runner {
    private static final Logger LOG = LogManager.getLogger(FullAndIncreStarter.class);

    private Pumper fullPumper;

    private IncreLogPumper increLogPumper;

    private WaterGapPiping pumpPiping;

    private Drainer fullDrainer;

    private WaterGapPiping ackPiping;

    @Override
    public void init(WaterGapContext waterGapContext) {

        switch (waterGapContext.getGlobalConfig().getSourceConfig().getDatabaseType()) {
            case MySQL:
                LOG.info("# Init MySQL pumper...");
                fullPumper = new MysqlDBAllTypePumper();
                increLogPumper = new MysqlIncreLogPumper();
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
        if (increLogPumper.check()) {
            increLogPumper.prepare();
            LOG.info("### Incre Pumper Starter ###");
            increLogPumper.start();
            LOG.info("### Full Pumper Starter ###");
            fullPumper.pump();
            LOG.info("### Full Drainer Starter ###");
            fullDrainer.apply();
        } else {
            LOG.error("Incre pumper check fail");
        }

    }

    @Override
    public void waitForShutdown() throws InterruptedException {
        WaterGapLifeCycle pumpLife = (WaterGapLifeCycle) fullPumper;
        while (pumpLife.isStart()) {
            Thread.sleep(1000); // sleep 1s
        }
    }

    @Override
    public void destroy() {
        LOG.info("ready to destroy");
        super.stop();
        ((WaterGapLifeCycle) fullPumper).destroy();
        ((WaterGapLifeCycle) fullDrainer).destroy();
    }

}
