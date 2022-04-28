package org.min.watergap.intake.incre.rdbms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.context.WaterGapContext;
import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.intake.incre.IncreLogPumper;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Mysql增量拉取
 *
 * @Create by metaX.h on 2022/4/26 8:22
 */
public class MysqlIncreLogPumper extends IncreLogPumper {
    private static final Logger LOG = LogManager.getLogger(MysqlIncreLogPumper.class);

    @Override
    public void init(WaterGapContext waterGapContext) {
        super.init(waterGapContext);
    }

    @Override
    public void destroy() {

    }

    @Override
    public void pump() throws WaterGapException {

    }

    @Override
    public boolean check() {
        if (!checkPermissions()) {
            LOG.info("check permission fail");
            return false;
        }
        LOG.info("check permission success !!!");
        if (!checkBinlogOpen()) {
            LOG.info("check binlog open fail");
            return false;
        }
        LOG.info("check binlog open success !!!");
        if (!checkBinlogFormat()) {
            LOG.info("check binlog format fail");
            return false;
        }
        LOG.info("check binlog format success !!!");
        return true;
    }

    @Override
    public boolean prepare() {


        return false;
    }

    private static String CHECK_GRANT_SQL = "SHOW GRANTS FOR %s@'%s'";
    protected boolean checkPermissions() {
        AtomicBoolean result = new AtomicBoolean(false);
        String sql = String.format(CHECK_GRANT_SQL, waterGapContext.getGlobalConfig().getSourceConfig().getUser(),
                waterGapContext.getGlobalConfig().getSourceConfig().getIp());
        try {
            executeQuery(sql, (resultSet) -> {
                while (resultSet.next()) {
                    String grants = resultSet.getString(1);
                    if (grants.contains("ALL PRIVILEGES")) {
                        result.set(true);
                    } else if (grants.contains("REPLICATION SLAVE") && grants.contains("REPLICATION CLIENT")) {
                        result.set(true);
                    }
                }
            });
        } catch (Exception e) {
            LOG.error("checkPermissions error, sql : " + sql, e);
        }

        return result.get();
    }

    protected boolean checkBinlogOpen() {

        AtomicBoolean result = new AtomicBoolean(false);
        String sql = "SHOW VARIABLES LIKE 'log_bin';";
        try {
            executeQuery(sql, (resultSet) -> {
                while (resultSet.next()) {
                    String grants = resultSet.getString(2);
                    if (grants.contains("ON")) {
                        result.set(true);
                    }
                }
            });
        } catch (Exception e) {
            LOG.error("checkBinlogOpen error, sql : " + sql, e);
        }

        return result.get();
    }

    protected boolean checkBinlogFormat() {
        AtomicBoolean result = new AtomicBoolean(false);
        String sql = "SHOW VARIABLES LIKE 'binlog_format';";
        try {
            executeQuery(sql, (resultSet) -> {
                while (resultSet.next()) {
                    String grants = resultSet.getString(2);
                    if (grants.contains("ROW")) {
                        result.set(true);
                    }
                }
            });
        } catch (Exception e) {
            LOG.error("checkBinlogOpen error, sql : " + sql, e);
        }

        return result.get();
    }
}
