package org.min.watergap.intake.dialect;

/**
 * mysql 说需要的语句
 *
 * @Create by metaX.h on 2021/11/11 23:23
 */
public class MysqlDialect extends DBDialect {

    @Override
    public String SHOW_DATABASES() {
        return "SHOW DATABASES;";
    }

    @Override
    public String SHOW_TABLES() {
        return "SHOW TABLES;";
    }

    @Override
    public String SHOW_CREATE_TABLE(String tableName) {
        return "SHOW CREATE TABLE " + tableName;
    }
}
