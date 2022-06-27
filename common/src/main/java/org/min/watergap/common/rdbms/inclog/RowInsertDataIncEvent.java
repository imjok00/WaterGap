package org.min.watergap.common.rdbms.inclog;

/**
 * 行数据类型
 *
 * @Create by metaX.h on 2022/6/5 13:18
 */
public class RowInsertDataIncEvent extends BaseIncEvent {

    private String schemaName;

    private String tableName;

    private ColumnInfo[] columnInfos;

    public RowInsertDataIncEvent(String schemaName, String tableName) {
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

    public ColumnInfo[] getColumnInfos() {
        return columnInfos;
    }

    public void setColumnInfos(ColumnInfo[] columnInfos) {
        this.columnInfos = columnInfos;
    }
}
