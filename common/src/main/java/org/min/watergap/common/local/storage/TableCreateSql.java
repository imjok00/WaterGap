package org.min.watergap.common.local.storage;

/**
 * 建表语句汇总
 *
 * @Create by metaX.h on 2021/12/13 23:21
 */
public class TableCreateSql {

    public static final String CREATE_FULL_EXTRACTOR_STATUS_TABLE =
            "CREATE TABLE IF NOT EXISTS FULL_EXTRACTOR_STATUS (" +
            "id BIGINT PRIMARY KEY     NOT NULL," +
            "schema VARCHAR(255)," +
            "tableName VARCHAR(255)," +
            "currentOffset BIGINT," +
            "status INT," +
            "createSql TEXT," +
            "primaryKeys VARCHAR(512)" +
            ")"
            ;
    public static final String CREATE_FULL_EXTRACTOR_STATUS_INDEX =
            "CREATE INDEX IDX_FULL_EXTRACTOR_STATUS ON FULL_EXTRACTOR_STATUS(tableName)";
}
