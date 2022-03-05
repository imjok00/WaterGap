package org.min.watergap.common.piping.data.impl;

import org.min.watergap.common.local.storage.entity.AbstractLocalStorageEntity;
import org.min.watergap.common.local.storage.entity.FullSchemaStatus;
import org.min.watergap.common.rdbms.struct.StructType;

/**
 * schema中间对象
 *
 * @Create by metaX.h on 2022/2/23 23:29
 */
public class SchemaStructBasePipingData extends StructBasePipingData {

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
    public String generateInsertSQL() {
        return new FullSchemaStatus(this.name, AbstractLocalStorageEntity.LocalStorageStatus.INIT).generateInsert();
    }
}
