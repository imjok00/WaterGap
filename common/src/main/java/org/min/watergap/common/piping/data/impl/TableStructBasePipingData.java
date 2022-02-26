package org.min.watergap.common.piping.data.impl;

import org.min.watergap.common.rdbms.struct.StructType;

/**
 * 表结构中间对象
 *
 * @Create by metaX.h on 2022/2/26 9:03
 */
public class TableStructBasePipingData extends StructBasePipingData {

    private String schemaName;

    private String tableName;

    public TableStructBasePipingData(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    @Override
    public StructType getType() {
        return StructType.TABLE;
    }

}
