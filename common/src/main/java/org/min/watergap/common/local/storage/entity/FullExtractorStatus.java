package org.min.watergap.common.local.storage.entity;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * 全量迁移一张表的当前状态
 *
 * @Create by metaX.h on 2021/11/7 15:43
 */
public class FullExtractorStatus extends AbstractLocalStorageEntity {

    public static List<String> ALL_COLUMNS
            = Arrays.asList("id", "schema", "tableName", "currentOffset", "status", "createSql", "primaryKeys");

    public static String MAPPING_LOCAL_TABLE_NAME = "FULL_EXTRACTOR_STATUS";

    // 主键
    private Long id;
    // 数据库schema
    private String schema;
    // 数据库表名
    private String tableName;
    // 当前迁移到的offset
    private String currentOffset;
    // 迁移状态 0：初始化 1：迁移中 2：迁移完成
    private int status;
    // 建表语句
    private String createSql;
    // 待迁移表的主键
    private String primaryKeys;

    public FullExtractorStatus() {

    }

    public FullExtractorStatus(String schema, String tableName, String currentOffset,
                               LocalStorageStatus status, String createSql, String primaryKeys) {
        this.schema = schema;
        this.tableName = tableName;
        this.currentOffset = currentOffset;
        this.status = status.getStatus();
        this.createSql = createSql;
        this.primaryKeys = primaryKeys;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getCurrentOffset() {
        return currentOffset;
    }

    public void setCurrentOffset(String currentOffset) {
        this.currentOffset = currentOffset;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getCreateSql() {
        return createSql;
    }

    public void setCreateSql(String createSql) {
        this.createSql = createSql;
    }

    public String getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(String primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    @Override
    public String getLocalTableName() {
        return MAPPING_LOCAL_TABLE_NAME;
    }

    public List<String> getSelectColumns() {
        return ALL_COLUMNS;
    }

    @Override
    public AbstractLocalStorageEntity generateObj(ResultSet resultSet) throws SQLException {
        FullExtractorStatus fullExtractorStatus = new FullExtractorStatus();
        fullExtractorStatus.setId(resultSet.getLong("id"));
        fullExtractorStatus.setSchema(resultSet.getString("schema"));
        fullExtractorStatus.setTableName(resultSet.getString("tableName"));
        fullExtractorStatus.setCurrentOffset(resultSet.getString("currentOffset"));
        fullExtractorStatus.setStatus(resultSet.getInt("status"));
        fullExtractorStatus.setCreateSql(resultSet.getString("createSql"));
        fullExtractorStatus.setPrimaryKeys(resultSet.getString("primaryKeys"));
        return fullExtractorStatus;
    }
}
