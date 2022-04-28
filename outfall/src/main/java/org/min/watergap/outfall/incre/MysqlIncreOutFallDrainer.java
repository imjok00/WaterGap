package org.min.watergap.outfall.incre;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.outfall.OutFallDrainer;
import org.min.watergap.piping.translator.PipingData;

/**
 * 关系型数据库执行器
 *
 * @Create by metaX.h on 2022/3/4 22:55
 */
public class MysqlIncreOutFallDrainer extends OutFallDrainer {
    private static final Logger LOG = LogManager.getLogger(MysqlIncreOutFallDrainer.class);


    @Override
    protected void doExecute(PipingData dataStruct) {

    }

    @Override
    public void destroy() {
        super.destroy();
    }
}
