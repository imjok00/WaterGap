package org.min.watergap.intake.dialect;

import org.min.watergap.common.config.DatabaseType;

/**
 * Todo
 *
 * @Create by metaX.h on 2022/2/21 23:59
 */
public class DBDialectWrapper extends DBDialect{

    private DBDialect dbDialect;

    public DBDialectWrapper(DatabaseType databaseType) {
        switch (databaseType) {
            case MYSQL:
                dbDialect = new MysqlDialect();
        }
    }

    @Override
    public String SHOW_TABLES() {
        return dbDialect.SHOW_TABLES();
    }

    @Override
    public String SHOW_DATABASES() {
        return dbDialect.SHOW_DATABASES();
    }

    @Override
    public String SHOW_CREATE_TABLE(String tableName) {
        return dbDialect.SHOW_CREATE_TABLE(tableName);
    }
}
