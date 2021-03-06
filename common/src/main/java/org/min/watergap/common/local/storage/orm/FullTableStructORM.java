package org.min.watergap.common.local.storage.orm;

import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.table.DatabaseTable;

/**
 * 记录数据库表结构
 *
 * @Create by metaX.h on 2022/4/5 10:09
 */
@DatabaseTable(tableName = "FULL_TABLE_STRUCT")
public class FullTableStructORM {

    @DatabaseField(columnName = "id", generatedId = true)
    private Long id;

    @DatabaseField(columnName="schemaName")
    private String schemaName;

    @DatabaseField(columnName="tableName")
    private String tableName;

    @DatabaseField(columnName="sourceCreateSql")
    private String sourceCreateSql;

    @DatabaseField(columnName="columns")
    private String columns;

    @DatabaseField(columnName="indexInfo")
    private String indexInfo;

    @DatabaseField(columnName="options")
    private String options;

    @DatabaseField(columnName="status")
    private int status;

    public FullTableStructORM() {
    }

    public static FullTableStructORM build() {
        return new FullTableStructORM();
    }

    public FullTableStructORM schemaName(String schemaName) {
        this.schemaName = schemaName;
        return this;
    }

    public FullTableStructORM tableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public FullTableStructORM sourceCreateSql(String sourceCreateSql) {
        this.sourceCreateSql = sourceCreateSql;
        return this;
    }

    public FullTableStructORM columns(String columns) {
        this.columns = columns;
        return this;
    }

    public FullTableStructORM indexInfo(String indexInfo) {
        this.indexInfo = indexInfo;
        return this;
    }

    public FullTableStructORM options(String options) {
        this.options = options;
        return this;
    }


    public FullTableStructORM status(int status) {
        this.status = status;
        return this;
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

    public String getColumns() {
        return columns;
    }

    public void setColumns(String columns) {
        this.columns = columns;
    }

    public String getIndexInfo() {
        return indexInfo;
    }

    public void setIndexInfo(String indexInfo) {
        this.indexInfo = indexInfo;
    }

    public String getOptions() {
        return options;
    }

    public void setOptions(String options) {
        this.options = options;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }
}
