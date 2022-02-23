package org.min.watergap.common.piping.data.impl;

import org.min.watergap.common.piping.PipingData;
import org.min.watergap.common.rdbms.struct.StructType;

/**
 * 传输对象
 *
 * @Create by metaX.h on 2022/2/23 23:24
 */
public abstract class BasePipingData implements PipingData {

    private StructType type;

    public abstract StructType getType();

}
