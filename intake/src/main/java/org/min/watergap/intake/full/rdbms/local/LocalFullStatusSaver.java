package org.min.watergap.intake.full.rdbms.local;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.min.watergap.common.local.storage.SqliteUtils;
import org.min.watergap.common.local.storage.entity.AbstractLocalStorageEntity;

import java.sql.SQLException;

/**
 * 存储本地执行状态到本地库
 *
 * @Create by metaX.h on 2021/12/14 23:32
 */
public class LocalFullStatusSaver {
    private static final Logger LOG = LogManager.getLogger(LocalFullStatusSaver.class);

    public boolean save(AbstractLocalStorageEntity entity) {
        try {
            return SqliteUtils.executeSQL(entity.generateInsert());
        } catch (SQLException e) {
            LOG.info("execute sql : {} error", entity.generateInsert());
        }
    }

}
