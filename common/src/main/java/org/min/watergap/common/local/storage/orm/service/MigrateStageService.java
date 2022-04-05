package org.min.watergap.common.local.storage.orm.service;

import com.j256.ormlite.dao.Dao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.local.storage.OrmJdbcHelper;
import org.min.watergap.common.local.storage.orm.MigrateStageORM;

import java.sql.SQLException;

/**
 *
 *
 * @Create by metaX.h on 2022/4/5 20:46
 */
public class MigrateStageService {
    private static final Logger LOG = LogManager.getLogger(MigrateStageService.class);

    private Dao<MigrateStageORM, Long> dao;

    public MigrateStageService() {
        this.dao = OrmJdbcHelper.getDaoSupport(MigrateStageORM.class);
    }

    public MigrateStageORM queryOne() {
        try {
            return this.dao.queryForFirst();
        } catch (SQLException e) {
            LOG.error("query migrate status error");
        }
        return null;
    }

    public void create() {
        try {
            MigrateStageORM orm = new MigrateStageORM(MigrateStageORM.StageEnum.START.toString());
            this.dao.create(orm);
        } catch (SQLException e) {
            LOG.error("create migrate stage error", e);
        }
    }

    public int updateStage(String stage) {
        MigrateStageORM orm = this.queryOne();
        orm.setStage(stage);
        try {
            return this.dao.update(orm);
        } catch (SQLException e) {
            LOG.error("update stage : {} error", stage, e);
        }
        return 0;
    }

    public static enum LocalStorageStatus {
        INIT(0), COMPLETE(1);

        private int status;

        LocalStorageStatus(int status) {
            this.status = status;
        }

        public int getStatus() {
            return status;
        }

        public static LocalStorageStatus valueOf(int value) {
            switch (value) {
                case 0:
                    return INIT;
                case 1:
                    return COMPLETE;
            }
            return null;
        }

        @Override
        public String toString() {
            return "LocalStorageStatus{" +
                    "status=" + status +
                    '}';
        }
    }

}
