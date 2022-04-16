package org.min.watergap.piping;

import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.piping.translator.PipingData;

/**
 * 数据处理器抽象
 *
 * @Create by metaX.h on 2022/4/9 9:32
 */
public interface PipingExecutor {

    int execute(PipingData pipingData) throws WaterGapException;

}
