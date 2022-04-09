package org.min.watergap.common.piping.struct.impl;

import org.min.watergap.common.rdbms.struct.StructType;

/**
 * schema中间对象
 *
 * @Create by metaX.h on 2022/2/23 23:29
 */
public class SchemaStructBasePipingData extends RdbmsStructBasePipingData {

    private String name;

    public SchemaStructBasePipingData(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public StructType getType() {
        return StructType.SCHEMA;
    }

    @Override
    public String toString() {
        return "SchemaStructBasePipingData{" +
                "name='" + name + '\'' +
                '}';
    }
}
