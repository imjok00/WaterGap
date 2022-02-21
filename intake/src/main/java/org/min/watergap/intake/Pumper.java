package org.min.watergap.intake;

import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.common.lifecycle.WaterGapLifeCycle;

/**
 * 抽取泵
 *
 * @Create by metaX.h on 2021/11/4 23:27
 */
public interface Pumper extends WaterGapLifeCycle {

    void pump() throws WaterGapException;

}

