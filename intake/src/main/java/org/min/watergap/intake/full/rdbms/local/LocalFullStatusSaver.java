package org.min.watergap.intake.full.rdbms.local;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.local.storage.SqliteUtils;
import org.min.watergap.common.local.storage.entity.AbstractLocalStorageEntity;
import org.min.watergap.common.piping.PipingData;

import java.sql.SQLException;

/**
 * 存储本地执行状态到本地库
 *
 * @Create by metaX.h on 2021/12/14 23:32
 */
public class LocalFullStatusSaver {
    private static final Logger LOG = LogManager.getLogger(LocalFullStatusSaver.class);

    public static boolean save(PipingData entity) throws SQLException {
        try {
            return SqliteUtils.executeSQL(entity.generateSQL());
        } catch (SQLException e) {
            LOG.error("execute sql : {} error", entity.generateSQL());
            throw e;
        }
    }

    public static AbstractLocalStorageEntity switchPipingDataToStoreEntity(PipingData pipingData) {
        return null;
    }

}
