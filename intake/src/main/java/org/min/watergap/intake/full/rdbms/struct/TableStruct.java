package org.min.watergap.intake.full.rdbms.struct;

import java.util.List;

/**
 * 作为关系形数据库的表结构基本传输单元
 *
 * @Create by metaX.h on 2021/11/14 19:13
 */
public class TableStruct extends BaseStruct {
    private String schema;
    private String table;
    private List<String> primaryKeys;
    /**
     * 存储表字段的信息，list的次序和数据库表字段次序一致，方便解析使用
     */
    private List<ColumnStruct> columnStructs;

    /**
     * 创建语句
     */
    private String createSql;

    public TableStruct(String schema, String table) {
        this.schema = schema;
        this.table = table;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public List<ColumnStruct> getColumnStructs() {
        return columnStructs;
    }

    public void setColumnStructs(List<ColumnStruct> columnStructs) {
        this.columnStructs = columnStructs;
    }
}
