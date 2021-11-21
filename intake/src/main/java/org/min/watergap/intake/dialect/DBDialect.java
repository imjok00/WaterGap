package org.min.watergap.intake.dialect;

/**
 * 各类数据库的方言
 *
 * @Create by metaX.h on 2021/11/13 22:19
 */
public abstract class DBDialect {

    public abstract String SHOW_TABLES();

    public abstract String SHOW_DATABASES();

    public abstract String SHOW_CREATE_TABLE(String tableName);
}
