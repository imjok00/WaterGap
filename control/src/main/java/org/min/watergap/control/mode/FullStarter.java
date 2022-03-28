package org.min.watergap.control.mode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.context.WaterGapContext;
import org.min.watergap.intake.Pumper;
import org.min.watergap.intake.full.rdbms.extractor.MysqlDBAllTypePumper;
import org.min.watergap.outfall.Drainer;
import org.min.watergap.outfall.RdbmsOutFallDrainer;

/**
 * 全量模式启动 <br/>
 * 只迁移全量数据
 *
 * @Create by metaX.h on 2021/11/3 23:52
 */
public class FullStarter implements Runner {
    private static final Logger LOG = LogManager.getLogger(FullStarter.class);

    private Pumper fullPumper;

    private Drainer fullDrainer;

    @Override
    public void init(WaterGapContext waterGapContext) {
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

        fullPumper.init(waterGapContext);
        fullDrainer.init(waterGapContext);
    }

    @Override
    public void start() {
        LOG.info("### Full Pumper Starter ###");
        fullPumper.pump();
        LOG.info("### Full Drainer Starter ###");
        fullDrainer.apply();
    }

    @Override
    public void destroy() {
        fullPumper.destroy();
        fullDrainer.destroy();
    }

    @Override
    public boolean isStart() {
        return fullPumper.isStart() && fullDrainer.isStart();
    }
}
