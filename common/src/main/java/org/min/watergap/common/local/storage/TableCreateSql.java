package org.min.watergap.common.local.storage;

/**
 * 建表语句汇总
 *
 * @Create by metaX.h on 2021/12/13 23:21
 */
public class TableCreateSql {

    public static final String CREATE_FULL_SCHEMA_STATUS_TABLE =
            "CREATE TABLE IF NOT EXISTS FULL_SCHEMA_STATUS (" +
            "schemaName VARCHAR(255)," +
            "status INT)";

    public static final String CREATE_FULL_TABLE_STATUS_TABLE =
            "CREATE TABLE IF NOT EXISTS FULL_TABLE_STATUS (" +
                    "schemaName VARCHAR(255)," +
                    "tableName VARCHAR(255)," +
                    "sourceCreateSql TEXT," +
                    "offset BIGINT," +
                    "status INT)";
}
