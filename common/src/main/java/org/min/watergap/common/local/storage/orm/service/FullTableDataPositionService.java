package org.min.watergap.common.local.storage.orm.service;

import com.j256.ormlite.stmt.QueryBuilder;
import com.j256.ormlite.stmt.UpdateBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.local.storage.OrmJdbcHelper;
import org.min.watergap.common.local.storage.orm.FullTableDataPositionORM;
import org.min.watergap.common.position.Position;
import org.min.watergap.common.utils.CollectionsUtils;

import java.sql.SQLException;
import java.util.List;

/**
 * 全量表数据迁移记录
 *
 * @Create by metaX.h on 2022/4/9 22:16
 */
public class FullTableDataPositionService extends BaseLocalService<FullTableDataPositionORM>{

    private static final Logger LOG = LogManager.getLogger(FullTableDataPositionService.class);

    public FullTableDataPositionService() {
        this.dao = OrmJdbcHelper.getDaoSupport(FullTableDataPositionORM.class);
    }

    public void create(FullTableDataPositionORM fullTableDataPositionORM) {
        try {
            fullTableDataPositionORM.setStatus(MigrateStageService.LocalStorageStatus.INIT.getStatus());
            dao.create(fullTableDataPositionORM);
        } catch (SQLException e) {
            LOG.error("create full position error, data : {} ", fullTableDataPositionORM, e);
        }
    }

    public String queryLastPosition(String schema, String table) throws SQLException {
        QueryBuilder queryBuilder = this.dao.queryBuilder();
        queryBuilder.where().eq("schemaName", schema)
                .and().eq("tableName", table);
        List<FullTableDataPositionORM> list = queryBuilder.query();
        if (CollectionsUtils.isNotEmpty(list)) {
            return list.get(0).getPosition();
        }
        return null;
    }

    public int updatePosition(Position position, String schema, String table) {
        try {
            UpdateBuilder updateBuilder = this.dao.updateBuilder();
            updateBuilder.updateColumnValue("position", position)
                    .where().eq("schemaName", schema)
                    .and().eq("tableName", table);
            LOG.info("table {}.{}, full data position {}", schema, table, position);
            return updateBuilder.update();
        } catch (SQLException e) {
            LOG.error("update table struct fail schema: {}, table : {}, position:{}",
                    schema, table, position, e);
        }
        return 0;
    }

    public int finishDataFull(String schema, String table) {
        try {
            UpdateBuilder updateBuilder = this.dao.updateBuilder();
            updateBuilder.updateColumnValue("status", MigrateStageService.LocalStorageStatus.COMPLETE.getStatus())
                    .where().eq("schemaName", schema)
                    .and().eq("tableName", table);
            LOG.info("table {}.{} full data complete", schema, table);
            return updateBuilder.update();
        } catch (SQLException e) {
            LOG.error("update table struct fail schema: {}, table : {}",
                    schema, table, e);
        }
        return 0;
    }

}
