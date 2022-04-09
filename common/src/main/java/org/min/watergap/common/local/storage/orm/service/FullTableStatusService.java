package org.min.watergap.common.local.storage.orm.service;

import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.stmt.UpdateBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.local.storage.OrmJdbcHelper;
import org.min.watergap.common.local.storage.orm.FullTableStatusORM;
import org.min.watergap.common.piping.struct.impl.TableStructBasePipingData;
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
public class FullTableStatusService {
    private static final Logger LOG = LogManager.getLogger(FullTableStatusService.class);

    private Dao<FullTableStatusORM, Long> dao;

    public FullTableStatusService() {
        this.dao = OrmJdbcHelper.getDaoSupport(FullTableStatusORM.class);
    }

    public FullTableStatusORM queryOne(String schema, String tableName) {
        Map<String, Object> condition = new HashMap<>();
        condition.put("schemaName", schema);
        condition.put("tableName", tableName);
        try {
            List<FullTableStatusORM> list = this.dao.queryForFieldValues(condition);
            if (CollectionsUtils.isNotEmpty(list)) {
                return list.get(0);
            }
        } catch (SQLException e) {
            LOG.error("query local status error , schema : {}, tableName : {}", schema, tableName, e);
        }
        return null;
    }

    public boolean create(FullTableStatusORM fullTableStatusORM) {
        try {
            return this.dao.create(fullTableStatusORM) > 0;
        } catch (SQLException e) {
            LOG.error("create local full status error, schema : {}, tableName : {}",
                    fullTableStatusORM.getSchemaName(), fullTableStatusORM.getTableName(), e);
        }
        return false;
    }

    public int update(TableStructBasePipingData pipingData, int status) {
        try {
            UpdateBuilder updateBuilder = this.dao.updateBuilder();
            updateBuilder.updateColumnValue("status", status)
                    .where().eq("schemaName", pipingData.getSchemaName())
                    .and().eq("tableName", pipingData.getTableName());
            return updateBuilder.update();
        } catch (SQLException e) {
            LOG.error("update table struct fail schema: {}, table : {}, status:{}",
                    pipingData.getSchemaName(), pipingData.getTableName(), status, e);
        }
        return 0;
    }

    public boolean create(TableStructBasePipingData pipingData) {
        try {
            FullTableStatusORM fullTableStatusORM = new FullTableStatusORM(pipingData.getSchemaName(),
                    pipingData.getTableName(),
                    pipingData.getSourceCreateSql(),
                    "",
                    MigrateStageService.LocalStorageStatus.INIT.getStatus());
            return this.dao.create(fullTableStatusORM) > 0;
        } catch (SQLException e) {
            LOG.error("create local full status error, schema : {}, tableName : {}",
                    pipingData.getSchemaName(), pipingData.getTableName(), e);
        }
        return false;
    }

}
