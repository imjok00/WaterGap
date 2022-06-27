package org.min.watergap.common.rdbms.inclog;

/**
 * 行数据类型
 *
 * @Create by metaX.h on 2022/6/5 13:18
 */
public class RowDelDataIncEvent implements IncEvent {

    private String schemaName;

    private String tableName;

    private ColumnInfo[] columnInfos;

    public RowDelDataIncEvent(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    static class ColumnInfo {
        public String name;
        public String value;
        public int sqlType;
        public boolean isKey;

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
