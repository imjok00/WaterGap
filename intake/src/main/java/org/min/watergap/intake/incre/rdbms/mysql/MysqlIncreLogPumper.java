package org.min.watergap.intake.incre.rdbms.mysql;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.context.WaterGapContext;
import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.common.local.storage.orm.IncrePositionORM;
import org.min.watergap.common.local.storage.orm.service.IncrePositionService;
import org.min.watergap.common.position.Position;
import org.min.watergap.common.position.incre.MysqlIncrePosition;
import org.min.watergap.common.utils.StringUtils;
import org.min.watergap.intake.incre.IncreLogPumper;
import org.min.watergap.intake.incre.rdbms.mysql.event.EventProcess;
import org.min.watergap.piping.thread.SingleThreadWorkGroup;
import org.min.watergap.piping.translator.WaterGapPiping;
import org.min.watergap.piping.translator.impl.IncreLogPositionBasePipingData;

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

    private WaterGapPiping increMPiping;

    @Override
    public void init(WaterGapContext waterGapContext) {
        super.init(waterGapContext);
        increPositionService = new IncrePositionService();
        increMPiping = new WaterGapPiping();
    }

    @Override
    public void destroy() {
        workGroup.destroy();
    }

    @Override
    public void pump() throws WaterGapException {
        workGroup = new SingleThreadWorkGroup(pipingData -> {
            BinaryLogClient client = getBinaryLogClient();
            try {
                sendPosition(client, (IncreLogPositionBasePipingData) pipingData);
            } catch (IOException e) {
                throw new WaterGapException("send position to db error", e);
            }
            return 0;
        }, increMPiping);
        try {
            increMPiping.put(new IncreLogPositionBasePipingData(getLastIncrePosition()));
        } catch (InterruptedException interruptedException) {
            LOG.error("can not get incre log position, be interrupt ", interruptedException);
            destroy();
        }

    }

    private Position getLastIncrePosition() {
        IncrePositionORM increPositionORM = increPositionService.queryOne();
        MysqlIncrePosition increPosition = new MysqlIncrePosition();
        increPosition.parse(increPositionORM.getPosition());
       return increPosition;
    }

    private void sendPosition(BinaryLogClient client, IncreLogPositionBasePipingData positionBasePipingData) throws IOException {
        MysqlIncrePosition increPosition = (MysqlIncrePosition) positionBasePipingData.getPosition();
        if (increPosition.isGtidMode()) {
            client.setGtidSet(increPosition.getVal());
            client.setUseBinlogFilenamePositionInGtidMode(true);
        } else {
            client.setBinlogFilename(increPosition.getFile());
            client.setBinlogPosition(increPosition.getPosition());
        }
        client.registerEventListener(new EventProcess());
        client.connect();
    }

    private BinaryLogClient getBinaryLogClient() {
        return new BinaryLogClient(waterGapContext.getGlobalConfig().getSourceConfig().getIp(),
                waterGapContext.getGlobalConfig().getSourceConfig().getPort(),
                waterGapContext.getGlobalConfig().getSourceConfig().getUser(),
                waterGapContext.getGlobalConfig().getSourceConfig().getPassword());
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
