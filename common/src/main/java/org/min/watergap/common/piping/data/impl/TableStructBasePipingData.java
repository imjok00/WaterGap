package org.min.watergap.common.piping.data.impl;

import org.min.watergap.common.local.storage.entity.FullTableStatus;
import org.min.watergap.common.rdbms.struct.StructType;

import java.util.Map;

/**
 * 表结构中间对象
 *
 * @Create by metaX.h on 2022/2/26 9:03
 */
public class TableStructBasePipingData extends StructBasePipingData {

    private String schemaName;

    private String tableName;

    private String sourceCreateSql;

    public TableStructBasePipingData(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSourceCreateSql() {
        return sourceCreateSql;
    }

    public void setSourceCreateSql(String sourceCreateSql) {
        this.sourceCreateSql = sourceCreateSql;
    }

    @Override
    public StructType getType() {
        return StructType.TABLE;
    }

    @Override
    public String generateQuerySQL() {
        return new FullTableStatus(schemaName, tableName, sourceCreateSql).generateQueryOne();
    }

    @Override
    public String generateInsertSQL() {
        return new FullTableStatus(schemaName, tableName, sourceCreateSql).generateInsert();
    }

    @Override
    public String generateUpdateSQL(Map<String, Object> objectMap) {
        return new FullTableStatus(schemaName, tableName, sourceCreateSql).generateUpdate(objectMap);
    }
}
