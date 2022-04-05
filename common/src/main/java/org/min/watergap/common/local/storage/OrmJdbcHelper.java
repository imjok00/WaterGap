package org.min.watergap.common.local.storage;

import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.dao.DaoManager;
import com.j256.ormlite.jdbc.JdbcConnectionSource;
import com.j256.ormlite.table.TableUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;

/**
 * sqlite orm 工具类
 *
 * @Create by metaX.h on 2022/4/5 12:01
 */
public class OrmJdbcHelper {
    private static final Logger LOG = LogManager.getLogger(OrmJdbcHelper.class);
    private static String SQLITE_PATH = "jdbc:sqlite:sqlite.db";

    public static <T> Dao getDaoSupport(Class<T> clazz) {
        try {
            JdbcConnectionSource jdbcConnectionSource = new JdbcConnectionSource(SQLITE_PATH);
            TableUtils.createTableIfNotExists(jdbcConnectionSource, clazz);
            return DaoManager.createDao(jdbcConnectionSource, clazz);
        } catch (SQLException e) {
            LOG.error("Create Sqlite Dao Error", e);
        }
        return null;
    }

}
