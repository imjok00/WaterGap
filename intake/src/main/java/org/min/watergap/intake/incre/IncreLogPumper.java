package org.min.watergap.intake.incre;

import org.min.watergap.intake.Pumper;
import org.min.watergap.intake.full.DBPumper;

/**
 * 增量日志拉取类
 *
 * @Create by metaX.h on 2022/4/26 8:20
 */
public abstract class IncreLogPumper extends DBPumper implements Pumper {

    public abstract boolean check();

    public abstract boolean prepare();

}
