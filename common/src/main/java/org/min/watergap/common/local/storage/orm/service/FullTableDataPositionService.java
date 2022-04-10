package org.min.watergap.common.local.storage.orm.service;

import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.stmt.UpdateBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.local.storage.OrmJdbcHelper;
import org.min.watergap.common.local.storage.orm.FullTableDataPositionORM;
import org.min.watergap.common.piping.data.impl.FullTableDataBasePipingData;

import java.sql.SQLException;

/**
 * 全量表数据迁移记录
 *
 * @Create by metaX.h on 2022/4/9 22:16
 */
public class FullTableDataPositionService {

    private static final Logger LOG = LogManager.getLogger(FullTableDataPositionService.class);

    private Dao<FullTableDataPositionORM, Long> dao;

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

    public int updatePosition(FullTableDataBasePipingData data) {
        try {
            UpdateBuilder updateBuilder = this.dao.updateBuilder();
            updateBuilder.updateColumnValue("position", data.getPosition())
                    .where().eq("schemaName", data.getSchemaName())
                    .and().eq("tableName", data.getTableName());
            return updateBuilder.update();
        } catch (SQLException e) {
            LOG.error("update table struct fail schema: {}, table : {}, position:{}",
                    data.getSchemaName(), data.getTableName(),
                    data.getPosition(), e);
        }
        return 0;
    }

    public int finishDataFull(FullTableDataBasePipingData data) {
        try {
            UpdateBuilder updateBuilder = this.dao.updateBuilder();
            updateBuilder.updateColumnValue("status", MigrateStageService.LocalStorageStatus.COMPLETE.getStatus())
                    .where().eq("schemaName", data.getSchemaName())
                    .and().eq("tableName", data.getTableName());
            return updateBuilder.update();
        } catch (SQLException e) {
            LOG.error("update table struct fail schema: {}, table : {}, position:{}",
                    data.getSchemaName(), data.getTableName(),
                    data.getPosition(), e);
        }
        return 0;
    }
}
