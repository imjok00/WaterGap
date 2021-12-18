package org.min.watergap.common.local.storage.entity;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * 存储导出的schema信息
 *
 * @Create by metaX.h on 2021/12/14 23:38
 */
public class FullSchemaStatus extends AbstractLocalStorageEntity {

    public static List<String> ALL_COLUMNS
            = Arrays.asList("id", "schemaName", "status");

    private static final String Table_NAME = "FULL_SCHEMA_STATUS";

    private int id;

    private String schemaName;

    // 迁移状态 0：初始化 1：迁移中 2：迁移完成
    private int status;

    public int getId() {
        return id;
    }

    public void setId(int id) {
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

    @Override
    public String getLocalTableName() {
        return Table_NAME;
    }

    @Override
    public List<String> getSelectColumns() {
        return ALL_COLUMNS;
    }

    @Override
    public AbstractLocalStorageEntity generateObj(ResultSet resultSet) throws SQLException {
        FullSchemaStatus fullSchemaStatus = new FullSchemaStatus();
        fullSchemaStatus.setSchemaName(resultSet.getString("schemaName"));
        fullSchemaStatus.setStatus(resultSet.getInt("status"));
        return fullSchemaStatus;
    }

}
