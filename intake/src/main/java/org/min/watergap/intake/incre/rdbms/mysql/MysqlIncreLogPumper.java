package org.min.watergap.intake.incre.rdbms.mysql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.context.WaterGapContext;
import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.common.local.storage.orm.IncrePositionORM;
import org.min.watergap.common.local.storage.orm.service.IncrePositionService;
import org.min.watergap.common.position.Position;
import org.min.watergap.common.position.incre.MysqlGTIDSet;
import org.min.watergap.common.position.incre.MysqlIncrePosition;
import org.min.watergap.common.utils.StringUtils;
import org.min.watergap.intake.incre.IncreLogPumper;
import org.min.watergap.intake.incre.rdbms.mysql.connect.MysqlConnection;
import org.min.watergap.piping.thread.SingleThreadWorkGroup;
import org.min.watergap.piping.translator.impl.IncreLogPositionPipingData;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Mysql增量拉取
 *
 * @Create by metaX.h on 2022/4/26 8:22
 */
public class MysqlIncreLogPumper extends IncreLogPumper {
    private static final Logger LOG = LogManager.getLogger(MysqlIncreLogPumper.class);

    private IncrePositionService increPositionService;

    private SingleThreadWorkGroup workGroup;

    private MysqlConnection mysqlConnection;

    @Override
    public void init(WaterGapContext waterGapContext) {
        super.init(waterGapContext);
        increPositionService = new IncrePositionService();
        mysqlConnection = new MysqlConnection(waterGapContext);
    }

    @Override
    public void destroy() {
        try {
            mysqlConnection.disconnect();
        } catch (IOException e) {
            LOG.error("mysql connect disconnect fail", e);
        }

        workGroup.destroy();
    }

    @Override
    public void pump() throws WaterGapException {
        startPipingThread();
    }

    private void startPipingThread() {
        new Thread(() -> {
            Position position = getLastIncrePosition();
            try {
                sendPosition((IncreLogPositionPipingData) position);
            } catch (Exception e) {
                LOG.error("start fetch incre log error ", e);
                destroy();
            }
        }).start();
    }

    private Position getLastIncrePosition() {
        IncrePositionORM increPositionORM = increPositionService.queryOne();
        MysqlIncrePosition increPosition = new MysqlIncrePosition();
        increPosition.parse(increPositionORM.getPosition());
       return increPosition;
    }

    private void sendPosition(IncreLogPositionPipingData positionBasePipingData) throws Exception {
        MysqlIncrePosition increPosition = (MysqlIncrePosition) positionBasePipingData.getPosition();

        if (increPosition.isGtidMode()) {
            mysqlConnection.dump((MysqlGTIDSet) increPosition.getUuidSet(), increMPiping);
        } else {
            mysqlConnection.dump(increPosition.getFile(), increPosition.getPosition(), increMPiping);
        }

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

        IncrePositionORM increPositionORM = increPositionService.queryOne();
        if (increPositionORM == null) {
            // 准备记录增量的位点
            String sql = "SHOW MASTER STATUS;";
            try {
                executeQuery(sql, resultSet -> {
                    while (resultSet.next()) {
                        String file = resultSet.getString("File");
                        Long position = resultSet.getLong("Position");
                        String gtidSet = resultSet.getString("Executed_Gtid_Set");
                        if (StringUtils.isNotEmpty(gtidSet)) {
                            increPositionService.create(new MysqlIncrePosition(file, gtidSet).toString());
                        } else {
                            increPositionService.create(new MysqlIncrePosition(file, position).toString());
                        }
                    }
                });
                return true;
            } catch (Exception e) {
                LOG.error("prepare error, sql : " + sql, e);
            }
        }

        return false;
    }

    private static String CHECK_GRANT_SQL = "SELECT * FROM mysql.user WHERE User = '%s'";
    protected boolean checkPermissions() {
        AtomicBoolean result = new AtomicBoolean(false);
        String sql = String.format(CHECK_GRANT_SQL, waterGapContext.getGlobalConfig().getSourceConfig().getUser());
        try {
            executeQuery(sql, (resultSet) -> {
                while (resultSet.next()) {
                    String slavePriv = resultSet.getString("Repl_slave_priv");
                    String clientPriv = resultSet.getString("Repl_client_priv");

                    if ("Y".equals(slavePriv) && "Y".equals(clientPriv)) {
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
