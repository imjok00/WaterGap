package org.min.watergap.common.local.storage.orm.service;

import com.j256.ormlite.stmt.UpdateBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.local.storage.OrmJdbcHelper;
import org.min.watergap.common.local.storage.orm.FullTableStructORM;
import org.min.watergap.common.utils.CollectionsUtils;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 *
 * @Create by metaX.h on 2022/4/5 20:11
 */
public class FullTableStructService extends BaseLocalService<FullTableStructORM> {
    private static final Logger LOG = LogManager.getLogger(FullTableStructService.class);

    public FullTableStructService() {
        this.dao = OrmJdbcHelper.getDaoSupport(FullTableStructORM.class);
    }

    public FullTableStructORM queryOne(String schema, String tableName) {
        Map<String, Object> condition = new HashMap<>();
        condition.put("schemaName", schema);
        condition.put("tableName", tableName);
        try {
            List<FullTableStructORM> list = this.dao.queryForFieldValues(condition);
            if (CollectionsUtils.isNotEmpty(list)) {
                return list.get(0);
            }
        } catch (SQLException e) {
            LOG.error("query local status error , schema : {}, tableName : {}", schema, tableName, e);
        }
        return null;
    }

    public boolean create(FullTableStructORM fullTableStatusORM) {
        try {
            return this.dao.create(fullTableStatusORM) > 0;
        } catch (SQLException e) {
            LOG.error("create local full status error, schema : {}, tableName : {}",
                    fullTableStatusORM.getSchemaName(), fullTableStatusORM.getTableName(), e);
        }
        return false;
    }

    public int update(String schema, String table, int status) {
        try {
            UpdateBuilder updateBuilder = this.dao.updateBuilder();
            updateBuilder.updateColumnValue("status", status)
                    .where().eq("schemaName", schema)
                    .and().eq("tableName", table);
            return updateBuilder.update();
        } catch (SQLException e) {
            LOG.error("update table struct fail schema: {}, table : {}, status:{}",
                    schema, table, status, e);
        }
        return 0;
    }

}
