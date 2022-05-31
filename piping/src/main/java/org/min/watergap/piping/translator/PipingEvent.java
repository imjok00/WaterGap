package org.min.watergap.piping.translator;

import org.min.watergap.common.rdbms.struct.StructType;
import org.min.watergap.piping.translator.type.PipingType;

/**
 * 中间传输事件对象
 *
 * @Create by metaX.h on 2022/5/23 0:36
 */
public interface PipingEvent extends PipingData {

    PipingType getPipingType();

    default StructType getType() {
        return StructType.INCRE_DATA;
    }

}
