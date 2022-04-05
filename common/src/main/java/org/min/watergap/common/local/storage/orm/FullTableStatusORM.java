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

    @DatabaseField(id=true)
    private Long id;

    private String schemaName;

    private String tableName;

    private String sourceCreateSql;

    private String pipingFields;

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
