package org.min.watergap.common.piping.data.impl;

import org.min.watergap.common.rdbms.struct.StructType;

/**
 * schema中间对象
 *
 * @Create by metaX.h on 2022/2/23 23:29
 */
public class SchemaStructBasePipingData extends StructBasePipingData {

    private String name;

    @Override
    public StructType getType() {
        return StructType.SCHEMA;
    }
}
