package org.min.watergap.common.local.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.datasource.config.DataSourceConfig;
import org.min.watergap.common.exception.LocalStorageException;
import org.min.watergap.common.local.storage.datasource.LocalStorageSource;
import org.min.watergap.common.local.storage.entity.AbstractLocalStorageEntity;
import org.min.watergap.common.utils.CollectionsUtils;
import org.sqlite.SQLiteException;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 操作本地sqlite公共方法类
 *
 * @Create by metaX.h on 2021/11/7 13:50
 */
public class SqliteUtils {
    private static final Logger LOG = LogManager.getLogger(SqliteUtils.class);

    private static final String SQLITE_JDBC_URL = "jdbc:sqlite:local.db";

    private static final String SQLITE_DRIVER_CLASS = "org.sqlite.JDBC";

    private static LocalStorageSource dataSource;

    static {
        DataSourceConfig dataSourceConfig = new DataSourceConfig() {
            @Override
            public String getJdbcUrl() {
                return SQLITE_JDBC_URL;
            }

            @Override
            public String getDriverClass() {
                return SQLITE_DRIVER_CLASS;
            }
        };
        dataSource = new LocalStorageSource(dataSourceConfig);
        prepare();
    }

    private static void prepare() {
        try {
            executeSQL(TableCreateSql.CREATE_FULL_TABLE_STATUS_TABLE);
            executeSQL(TableCreateSql.CREATE_FULL_SCHEMA_STATUS_TABLE);
        } catch (SQLiteException sqliteException) {
            LOG.warn("prepare init sqlite table error", sqliteException);
        } catch (SQLException sqlException) {
            LOG.warn("prepare sqlite error", sqlException);
        }
    }

    public List<AbstractLocalStorageEntity> selectLocalStorageByCondition(String contextId,
                                                                           AbstractLocalStorageEntity entity,
                                                                           HashMap<String, String> condition) throws LocalStorageException {
        List<AbstractLocalStorageEntity> results = new ArrayList<>();
        // 组装查询语句
        String sql = assembleSelectSql(entity, condition);
        try (
            Connection connection = DriverManager.getConnection(SQLITE_JDBC_URL);
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(sql)
        ) {
            while (rs.next()) {
                results.add(entity.generateObj(rs));
            }
            return results;
        } catch (Exception e) {
            throw new LocalStorageException("select " + entity.getLocalTableName() + " error", e);
        }
    }

    private String assembleSelectSql(AbstractLocalStorageEntity entity, HashMap<String, String> condition) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("select ").append(String.join(",",entity.getSelectColumns())).append(" from ")
                .append(entity.getLocalTableName());
        if (CollectionsUtils.isNotEmptyMap(condition)) {
            List<String> items = new ArrayList<>(condition.size());
            for (Map.Entry<String, String> entry : condition.entrySet()) {
                items.add(entry.getKey()+"='"+entry.getValue()+"'");
            }
            sqlBuilder.append(String.join(" and ", items));
        }
        return sqlBuilder.toString();
    }

    public static boolean executeSQL(String sql) throws SQLException {
        try (
                Connection connection = dataSource.getDataSource().getConnection();
                Statement statement = connection.createStatement();
        ) {
            return statement.execute(sql);
        }
    }

    public static Map<String, Object> executeQueryOne(String sql) throws SQLException {
        try (
                Connection connection = dataSource.getDataSource().getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql);
        ) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> map = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    map.put(metaData.getColumnName(i), resultSet.getObject(i));
                }
                return map;
            }
            return null;
        }
    }
}
