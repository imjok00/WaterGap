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
import org.min.watergap.intake.incre.rdbms.mysql.MysqlIncreLogPumper;
import org.min.watergap.outfall.Drainer;
import org.min.watergap.outfall.OutFallDrainer;
import org.min.watergap.outfall.full.RdbmsOutFallDrainer;
import org.min.watergap.outfall.incre.MysqlIncreOutFallDrainer;
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

    private WaterGapPiping increPiping;

    private Drainer fullDrainer;

    private Drainer incDrainer;

    private WaterGapPiping ackPiping;

    private WaterGapPiping increActPiping;

    private WaterGapContext waterGapContext;

    @Override
    public void init(WaterGapContext waterGapContext) {
        this.waterGapContext = waterGapContext;
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
                incDrainer = new MysqlIncreOutFallDrainer();
                break;
        }
        pumpPiping = new WaterGapPiping();
        ackPiping = new WaterGapPiping();
        increPiping = new WaterGapPiping();
        increActPiping = new WaterGapPiping();

        ((RdbmsDataPumper)fullPumper).injectPiping(pumpPiping, ackPiping);
        ((OutFallDrainer)fullDrainer).injectPiping(pumpPiping, ackPiping);
        ((WaterGapLifeCycle)fullPumper).init(waterGapContext);
        ((WaterGapLifeCycle)fullDrainer).init(waterGapContext);
        ((WaterGapLifeCycle)increLogPumper).init(waterGapContext);
        ((WaterGapLifeCycle)incDrainer).init(waterGapContext);
        increLogPumper.injectPiping(increPiping, increActPiping);
        ((OutFallDrainer)incDrainer).injectPiping(increPiping, increActPiping);
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
            LOG.info("### Incre Drainer Starter ###");
            incDrainer.apply();
        } else {
            LOG.error("Incre pumper check fail");
        }

    }

    @Override
    public void waitForShutdown() throws InterruptedException {
        waterGapContext.getFullCntLatch().await();
        // 全量结束
        ((WaterGapLifeCycle) fullPumper).destroy();
        // 增量开始
        increLogPumper.pump();
    }

    @Override
    public void destroy() {
        LOG.info("ready to destroy");
        super.stop();
        ((WaterGapLifeCycle) fullPumper).destroy();
        ((WaterGapLifeCycle) fullDrainer).destroy();
    }

}
