package org.min.watergap.common.piping.struct.impl;

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
public class TableStructBasePipingData extends RdbmsStructBasePipingData {

    private List<Column> columns;

    private IndexInfo indexInfo;

    public TableStructBasePipingData(String schemaName, String tableName) {
        setSchemaName(schemaName);
        setTableName(tableName);
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public IndexInfo getIndexInfo() {
        return indexInfo;
    }

    public void setIndexInfo(IndexInfo indexInfo) {
        this.indexInfo = indexInfo;
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
        return new FullTableStatus(getSchemaName(), getTableName()).generateQueryOne();
    }

    @Override
    public String generateInsertSQL() {
        return new FullTableStatus(getSchemaName(), getTableName(), getSourceCreateSql(),
                AbstractLocalStorageEntity.LocalStorageStatus.INIT.getStatus()).generateInsert();
    }

    @Override
    public String generateUpdateSQL(Map<String, Object> objectMap) {
        return new FullTableStatus(getSchemaName(), getTableName()).generateUpdate(objectMap);
    }

    public static class Column {
        private String columnName;
        private int columnType;
        private String typeName;

        public Column(String columnName, int columnType, String typeName) {
            this.columnName = columnName;
            this.columnType = columnType;
            this.typeName = typeName;
        }

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public int getColumnType() {
            return columnType;
        }

        public void setColumnType(int columnType) {
            this.columnType = columnType;
        }

        public String getTypeName() {
            return typeName;
        }

        public void setTypeName(String typeName) {
            this.typeName = typeName;
        }
    }


    public static class IndexInfo {

        private List<TableStructBasePipingData.Column> primaryKeys;

        private Map<String, List<TableStructBasePipingData.Column>> normalKeys;

        private Map<String, List<TableStructBasePipingData.Column>> uniqueKeys;

        private Map<String, List<TableStructBasePipingData.Column>> foreignKes;

        public List<Column> getPrimaryKeys() {
            return primaryKeys;
        }

        public void setPrimaryKeys(List<Column> primaryKeys) {
            this.primaryKeys = primaryKeys;
        }

        public Map<String, List<Column>> getNormalKeys() {
            return normalKeys;
        }

        public void setNormalKeys(Map<String, List<Column>> normalKeys) {
            this.normalKeys = normalKeys;
        }

        public Map<String, List<Column>> getUniqueKeys() {
            return uniqueKeys;
        }

        public void setUniqueKeys(Map<String, List<Column>> uniqueKeys) {
            this.uniqueKeys = uniqueKeys;
        }

        public Map<String, List<Column>> getForeignKes() {
            return foreignKes;
        }

        public void setForeignKes(Map<String, List<Column>> foreignKes) {
            this.foreignKes = foreignKes;
        }
    }

}
