package org.min.watergap.common.local.storage.orm.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.local.storage.OrmJdbcHelper;
import org.min.watergap.common.local.storage.orm.IncrePositionORM;

import java.sql.SQLException;

/**
 *
 *
 * @Create by metaX.h on 2022/4/5 20:46
 */
public class IncrePositionService extends BaseLocalService<IncrePositionORM> {
    private static final Logger LOG = LogManager.getLogger(IncrePositionService.class);

    public IncrePositionService() {
        this.dao = OrmJdbcHelper.getDaoSupport(IncrePositionORM.class);
    }

    public IncrePositionORM queryOne() {
        try {
            return this.dao.queryForFirst();
        } catch (SQLException e) {
            LOG.error("query migrate status error");
        }
        return null;
    }

    public void create(String position) {
        try {
            IncrePositionORM orm = new IncrePositionORM();
            orm.setPosition(position);
            this.dao.create(orm);
        } catch (SQLException e) {
            LOG.error("create migrate stage error", e);
        }
    }

    public int updatePosition(String position) {
        IncrePositionORM orm = this.queryOne();
        orm.setPosition(position);
        try {
            return this.dao.update(orm);
        } catch (SQLException e) {
            LOG.error("update position : {} error", position, e);
        }
        return 0;
    }

}
