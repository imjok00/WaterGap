package org.min.watergap.common.local.storage;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.piping.PipingData;

import java.sql.SQLException;

/**
 * 存储本地执行状态到本地库
 *
 * @Create by metaX.h on 2021/12/14 23:32
 */
public class LocalDataSaveTool {
    private static final Logger LOG = LogManager.getLogger(LocalDataSaveTool.class);

    public static boolean save(PipingData entity) throws SQLException {
        try {
            return SqliteUtils.executeSQL(entity.generateInsertSQL());
        } catch (SQLException e) {
            LOG.error("execute sql : {} error", entity.generateInsertSQL());
            throw e;
        }
    }

}
