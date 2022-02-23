package org.min.watergap.control.mode;

import org.min.watergap.common.config.WaterGapGlobalConfig;
import org.min.watergap.common.context.WaterGapContext;
import org.min.watergap.intake.Pumper;
import org.min.watergap.intake.full.rdbms.extractor.MysqlDBStructPumper;

/**
 * 全量模式启动 <br/>
 * 只迁移全量数据
 *
 * @Create by metaX.h on 2021/11/3 23:52
 */
public class FullStarter implements Runner {

    private Pumper fullPumper;

    @Override
    public void init(WaterGapContext waterGapContext) {
        switch (baseConfig.getSourceConfig().getDatabaseType()) {
            case MYSQL:
                fullPumper = new MysqlDBStructPumper();
                break;
        }
        fullPumper.in
    }

    @Override
    public void start() {
        fullPumper.pump();
    }

    @Override
    public void destroy() {

    }
}
