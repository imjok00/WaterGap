package org.min.watergap.intake;

import org.min.watergap.common.exception.WaterGapException;

/**
 * 抽取泵
 *
 * @Create by metaX.h on 2021/11/4 23:27
 */
public interface Pumper {

    void pump() throws WaterGapException;

}

