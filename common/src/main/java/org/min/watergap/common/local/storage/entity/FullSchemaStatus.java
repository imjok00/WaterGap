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
            = Arrays.asList("schemaName", COMMON_STATUS);

    private static final String TABLE_NAME = "FULL_SCHEMA_STATUS";

    private int id;

    private String schemaName;


    public FullSchemaStatus() {

    }

    public FullSchemaStatus(String schemaName) {
        this.schemaName = schemaName;
    }

    public FullSchemaStatus(String schemaName, LocalStorageStatus status) {
        this.schemaName = schemaName;
        setStatus(status.getStatus());
    }

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

    @Override
    public String getLocalTableName() {
        return TABLE_NAME;
    }

    @Override
    public List<String> getSelectColumns() {
        return ALL_COLUMNS;
    }

    @Override
    public String getSelectOneCondition() {
        return "schemaName = '" + schemaName + "'";
    }

    @Override
    public String getInsertValues() {
        return String.format("%s, %s", schemaName, getStatus());
    }

    @Override
    public AbstractLocalStorageEntity generateObj(ResultSet resultSet) throws SQLException {
        FullSchemaStatus fullSchemaStatus = new FullSchemaStatus();
        fullSchemaStatus.setSchemaName(resultSet.getString("schemaName"));
        fullSchemaStatus.setStatus(resultSet.getInt("status"));
        return fullSchemaStatus;
    }

}
