package org.min.watergap.intake.dialect;

/**
 * mysql 说需要的语句
 *
 * @Create by metaX.h on 2021/11/11 23:23
 */
public class MysqlDialect extends DBDialect {

    public static final String GET_ALL_SCHEMA = "SHOW DATABASES;";

    @Override
    public String SHOW_TABLES() {
        return "";
    }
}
