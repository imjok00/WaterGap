package org.min.watergap.common.local.storage.orm;

import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.table.DatabaseTable;

/**
 * 记录Schema对象
 *
 * @Create by metaX.h on 2022/4/5 15:20
 */
@DatabaseTable(tableName = "SCHEMA_STATUS")
public class SchemaStatusORM {

    @DatabaseField(id=true)
    private Long id;

    private String schemaName;

    private int status;

    public SchemaStatusORM() {
    }

    public SchemaStatusORM(String schemaName, int status) {
        this.schemaName = schemaName;
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

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }
}
