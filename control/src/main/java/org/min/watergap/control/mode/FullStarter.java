package org.min.watergap.control.mode;

import org.min.watergap.common.context.WaterGapContext;
import org.min.watergap.intake.Pumper;
import org.min.watergap.intake.full.rdbms.extractor.MysqlDBStructPumper;
import org.min.watergap.outfall.Drainer;
import org.min.watergap.outfall.RdbmsOutFallDrainer;

/**
 * 全量模式启动 <br/>
 * 只迁移全量数据
 *
 * @Create by metaX.h on 2021/11/3 23:52
 */
public class FullStarter implements Runner {

    private Pumper fullPumper;

    private Drainer fullDrainer;

    @Override
    public void init(WaterGapContext waterGapContext) {
        switch (waterGapContext.getGlobalConfig().getSourceConfig().getDatabaseType()) {
            case MySQL:
                fullPumper = new MysqlDBStructPumper();
                break;
        }
        switch (waterGapContext.getGlobalConfig().getTargetConfig().getDatabaseType()) {
            case MySQL:
                fullDrainer = new RdbmsOutFallDrainer();
                break;
        }

        fullPumper.init(waterGapContext);
        fullDrainer.init(waterGapContext);
    }

    @Override
    public void start() {
        fullPumper.pump();
        fullDrainer.apply();
    }

    @Override
    public void destroy() {
        fullPumper.destroy();
    }

    @Override
    public void isStart() {

    }
}
