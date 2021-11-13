package org.min.watergap.common.local.storage;

import org.min.watergap.common.exception.LocalStorageException;
import org.min.watergap.common.local.storage.entity.AbstractLocalStorageEntity;
import org.min.watergap.common.utils.CollectionsUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
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

    private static final String TEMPLATE_SQLITE_JDBC_URL = "jdbc:sqlite:%s.db";

    public List<AbstractLocalStorageEntity> selectLocalStorageByCondition (String contextId,
                                                                           AbstractLocalStorageEntity entity,
                                                                           HashMap<String, String> condition) throws LocalStorageException {
        List<AbstractLocalStorageEntity> results = new ArrayList<>();
        // 组装查询语句
        String sql = assembleSelectSql(entity, condition);
        try (
                Connection connection = DriverManager.getConnection(String.format(TEMPLATE_SQLITE_JDBC_URL, contextId));
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sql)
        ) {
            while (rs.next()) {
                results.add(entity.generateObj(rs));
            }
            return results;
        } catch (Exception e) {
            throw new LocalStorageException("select " + entity.getTableName() + " error", e);
        }
    }


    private String assembleSelectSql(AbstractLocalStorageEntity entity, HashMap<String, String> condition) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("select ").append(String.join(",",entity.getSelectColumns())).append(" from ")
                .append(entity.getTableName());
        if (CollectionsUtils.isNotEmptyMap(condition)) {
            List<String> items = new ArrayList<>(condition.size());
            for (Map.Entry<String, String> entry : condition.entrySet()) {
                items.add(entry.getKey()+"='"+entry.getValue()+"'");
            }
            sqlBuilder.append(String.join(" and ", items));
        }
        return sqlBuilder.toString();
    }
}
