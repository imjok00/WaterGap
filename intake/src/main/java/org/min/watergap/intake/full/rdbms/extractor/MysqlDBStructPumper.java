package org.min.watergap.intake.full.rdbms.extractor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.intake.full.rdbms.RdbmsDBStructPumper;

/**
 * mysql 全量数据导出类
 *
 * @Create by metaX.h on 2021/11/4 23:30
 */
public class MysqlDBStructPumper extends RdbmsDBStructPumper {
    private static final Logger LOG = LogManager.getLogger(MysqlDBStructPumper.class);

    @Override
    public void destroy() {

    }

    @Override
    public void isStart() {

    }
}
