package org.min.watergap.common.piping.data.impl;

import org.min.watergap.common.local.storage.entity.AbstractLocalStorageEntity;
import org.min.watergap.common.local.storage.entity.FullTableStatus;
import org.min.watergap.common.rdbms.struct.StructType;

import java.util.List;
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

    private List<Column> columns;

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

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public void addColumn(Column column) {
        this.columns.add(column);
    }

    @Override
    public StructType getType() {
        return StructType.TABLE;
    }

    @Override
    public String generateQuerySQL() {
        return new FullTableStatus(schemaName, tableName).generateQueryOne();
    }

    @Override
    public String generateInsertSQL() {
        return new FullTableStatus(schemaName, tableName, sourceCreateSql,
                AbstractLocalStorageEntity.LocalStorageStatus.INIT.getStatus()).generateInsert();
    }

    @Override
    public String generateUpdateSQL(Map<String, Object> objectMap) {
        return new FullTableStatus(schemaName, tableName).generateUpdate(objectMap);
    }


    public static class Column {
        private String name;
        private int type;

        public Column(String name, int type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getType() {
            return type;
        }

        public void setType(int type) {
            this.type = type;
        }
    }
}
