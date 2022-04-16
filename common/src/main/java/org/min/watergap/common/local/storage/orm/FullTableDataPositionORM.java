package org.min.watergap.common.local.storage.orm;

import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.table.DatabaseTable;

/**
 * 记录数据库表数据同步位置
 *
 * @Create by metaX.h on 2022/4/5 10:09
 */
@DatabaseTable(tableName = "FULL_TABLE_DATA")
public class FullTableDataPositionORM {

    @DatabaseField(columnName = "id", generatedId = true)
    private Long id;

    @DatabaseField(columnName="schemaName")
    private String schemaName;

    @DatabaseField(columnName="tableName")
    private String tableName;

    @DatabaseField(columnName="position")
    private String position;

    @DatabaseField(columnName="status")
    private int status;

    public FullTableDataPositionORM() {
    }

    public FullTableDataPositionORM(String schemaName, String tableName, String position) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.position = position;
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

    public String getPosition() {
        return position;
    }

    public void setPosition(String position) {
        this.position = position;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "FullTableDataPositionORM{" +
                "id=" + id +
                ", schemaName='" + schemaName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", position='" + position + '\'' +
                ", status=" + status +
                '}';
    }
}
