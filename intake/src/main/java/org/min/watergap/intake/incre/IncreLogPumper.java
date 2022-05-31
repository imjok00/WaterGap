package org.min.watergap.intake.incre;

import org.min.watergap.intake.Pumper;
import org.min.watergap.intake.full.DBPumper;
import org.min.watergap.piping.translator.WaterGapPiping;

/**
 * 增量日志拉取类
 *
 * @Create by metaX.h on 2022/4/26 8:20
 */
public abstract class IncreLogPumper extends DBPumper implements Pumper {

    protected WaterGapPiping increMPiping;

    public abstract boolean check();

    public abstract boolean prepare();

    public void injectIncrePiping(WaterGapPiping piping) {
        increMPiping = piping;
    }

}
