package org.min.watergap.common.piping.data.impl;

import org.min.watergap.common.local.storage.entity.AbstractLocalStorageEntity;
import org.min.watergap.common.local.storage.entity.FullSchemaStatus;
import org.min.watergap.common.rdbms.struct.StructType;

import java.util.Map;

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
    public String generateInsertSQL() {
        return new FullSchemaStatus(this.name, AbstractLocalStorageEntity.LocalStorageStatus.INIT).generateInsert();
    }

    @Override
    public String generateUpdateSQL(Map<String, Object> objectMap) {
        return new FullSchemaStatus(this.name).generateUpdate(objectMap);
    }

    @Override
    public String generateQuerySQL() {
        return new FullSchemaStatus(this.name).generateQueryOne();
    }
}
