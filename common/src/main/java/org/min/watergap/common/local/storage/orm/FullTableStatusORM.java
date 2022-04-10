package org.min.watergap.common.local.storage.orm;

import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.table.DatabaseTable;

/**
 * 记录数据库表结构
 *
 * @Create by metaX.h on 2022/4/5 10:09
 */
@DatabaseTable(tableName = "FULL_TABLE_STATUS")
public class FullTableStatusORM {

    @DatabaseField(columnName = "id", generatedId = true)
    private Long id;

    @DatabaseField(columnName="schemaName")
    private String schemaName;

    @DatabaseField(columnName="tableName")
    private String tableName;

    @DatabaseField(columnName="sourceCreateSql")
    private String sourceCreateSql;

    @DatabaseField(columnName="pipingFields")
    private String pipingFields;

    @DatabaseField(columnName="status")
    private int status;

    public FullTableStatusORM() {
    }

    public FullTableStatusORM(String schemaName, String tableName, String sourceCreateSql, String pipingFields, int status) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.sourceCreateSql = sourceCreateSql;
        this.pipingFields = pipingFields;
        this.status = status;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
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

    public String getPipingFields() {
        return pipingFields;
    }

    public void setPipingFields(String pipingFields) {
        this.pipingFields = pipingFields;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }
}
