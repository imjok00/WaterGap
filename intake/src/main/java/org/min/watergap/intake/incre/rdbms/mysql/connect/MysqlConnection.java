package org.min.watergap.intake.incre.rdbms.mysql.connect;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.min.watergap.common.context.WaterGapContext;
import org.min.watergap.common.datasource.config.DataSourceConfig;
import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.common.position.incre.MysqlGTIDSet;
import org.min.watergap.common.rdbms.driver.mysql.MysqlConnector;
import org.min.watergap.common.rdbms.driver.mysql.MysqlQueryExecutor;
import org.min.watergap.common.rdbms.driver.mysql.MysqlUpdateExecutor;
import org.min.watergap.common.rdbms.driver.mysql.dbsync.DirectLogFetcher;
import org.min.watergap.common.rdbms.driver.mysql.packets.HeaderPacket;
import org.min.watergap.common.rdbms.driver.mysql.packets.client.BinlogDumpCommandPacket;
import org.min.watergap.common.rdbms.driver.mysql.packets.client.BinlogDumpGTIDCommandPacket;
import org.min.watergap.common.rdbms.driver.mysql.packets.client.SemiAckCommandPacket;
import org.min.watergap.common.rdbms.driver.mysql.packets.server.ResultSetPacket;
import org.min.watergap.common.rdbms.driver.mysql.utils.PacketManager;
import org.min.watergap.common.rdbms.misc.binlog.BaseLogEvent;
import org.min.watergap.common.rdbms.misc.binlog.LogBuffer;
import org.min.watergap.common.rdbms.misc.binlog.LogContext;
import org.min.watergap.common.rdbms.misc.binlog.LogDecoder;
import org.min.watergap.piping.convertor.mysql.MysqlIncreLogConvert;
import org.min.watergap.piping.thread.MultiThreadWorkGroup;
import org.min.watergap.piping.translator.WaterGapPiping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MysqlConnection {

    private static final Logger logger         = LoggerFactory.getLogger(MysqlConnection.class);

    private MysqlConnector connector;
    private long                slaveId;
    private Charset             charset        = Charset.forName("UTF-8");
    private BinlogFormat        binlogFormat;
    private BinlogImage         binlogImage;

    // tsdb releated
    private DataSourceConfig authInfo;
    protected int               connTimeout    = 5 * 1000;                                      // 5秒
    protected int               soTimeout      = 60 * 60 * 1000;                                // 1小时
    private int                 binlogChecksum = BaseLogEvent.BINLOG_CHECKSUM_ALG_OFF;
    // Master heartbeat interval
    public static final int       MASTER_HEARTBEAT_PERIOD_SECONDS = 15;
    // dump binlog bytes, 暂不包括meta与TSDB
    private AtomicLong          receivedBinlogBytes;

    public MysqlConnection(WaterGapContext waterGapContext){
        authInfo = waterGapContext.getGlobalConfig().getSourceConfig();
        InetSocketAddress inetSocketAddress = new InetSocketAddress(authInfo.getIp(), authInfo.getPort());
        connector = new MysqlConnector(inetSocketAddress, authInfo.getUser(), authInfo.getPassword());
        // 将connection里面的参数透传下
        connector.setSoTimeout(soTimeout);
        connector.setConnTimeout(connTimeout);
        connector.setDefaultSchema(authInfo.getDatabaseName());
    }

    public void connect() throws IOException {
        connector.connect();
    }

    public void reconnect() throws IOException {
        connector.reconnect();
    }

    public void disconnect() throws IOException {
        connector.disconnect();
    }

    public boolean isConnected() {
        return connector.isConnected();
    }

    public void update(String cmd) throws IOException {
        MysqlUpdateExecutor exector = new MysqlUpdateExecutor(connector);
        exector.update(cmd);
    }

    public ResultSetPacket query(String cmd) throws IOException {
        MysqlQueryExecutor exector = new MysqlQueryExecutor(connector);
        return exector.query(cmd);
    }


    public void dump(String binlogfilename, Long binlogPosition, WaterGapPiping piping) throws Exception {
        updateSettings();
        loadBinlogChecksum();
        sendBinlogDump(binlogfilename, binlogPosition);
        MysqlIncreLogConvert convert = new MysqlIncreLogConvert(Charset.defaultCharset());
        LogContext context = new LogContext();
        LogDecoder decoder = new LogDecoder(BaseLogEvent.UNKNOWN_EVENT, BaseLogEvent.ENUM_END_EVENT);
        try (DirectLogFetcher fetcher = new DirectLogFetcher(connector.getReceiveBufferSize())) {
            fetcher.start(connector.getChannel());
            while (fetcher.fetch()) {
                accumulateReceivedBytes(fetcher.limit());
                LogBuffer buffer = fetcher.duplicate();
                fetcher.consume(fetcher.limit());
                BaseLogEvent logEvent = decoder.decode(buffer, context);
                piping.put(convert.convert(logEvent));
            }
        }
    }

    public void dump(MysqlGTIDSet gtidSet, WaterGapPiping piping) throws Exception {
        updateSettings();
        loadBinlogChecksum();
        sendBinlogDumpGTID(gtidSet);
        MysqlIncreLogConvert convert = new MysqlIncreLogConvert(Charset.defaultCharset());
        LogContext context = new LogContext();
        LogDecoder decoder = new LogDecoder(BaseLogEvent.UNKNOWN_EVENT, BaseLogEvent.ENUM_END_EVENT);
        try (DirectLogFetcher fetcher = new DirectLogFetcher(connector.getReceiveBufferSize())) {
            fetcher.start(connector.getChannel());
            while (fetcher.fetch()) {
                accumulateReceivedBytes(fetcher.limit());
                LogBuffer buffer = fetcher.duplicate();
                fetcher.consume(fetcher.limit());
                BaseLogEvent logEvent = decoder.decode(buffer, context);
                piping.put(convert.convert(logEvent));
            }
        }
    }

    public void dump(MysqlGTIDSet gtidSet, MultiThreadWorkGroup worker) throws Exception {
        updateSettings();
        loadBinlogChecksum();
        sendBinlogDumpGTID(gtidSet);
        //MysqlIncreLogConvert convert = new MysqlIncreLogConvert(Charset.defaultCharset());
        LogContext context = new LogContext();
        LogDecoder decoder = new LogDecoder(BaseLogEvent.UNKNOWN_EVENT, BaseLogEvent.ENUM_END_EVENT);
        try (DirectLogFetcher fetcher = new DirectLogFetcher(connector.getReceiveBufferSize())) {
            fetcher.start(connector.getChannel());
            while (fetcher.fetch()) {
                accumulateReceivedBytes(fetcher.limit());
                LogBuffer buffer = fetcher.duplicate();
                fetcher.consume(fetcher.limit());
                BaseLogEvent logEvent = decoder.decode(buffer, context);
                worker.publish(logEvent);
            }
        }
    }


    private void sendBinlogDump(String binlogfilename, Long binlogPosition) throws IOException {
        BinlogDumpCommandPacket binlogDumpCmd = new BinlogDumpCommandPacket();
        binlogDumpCmd.binlogFileName = binlogfilename;
        binlogDumpCmd.binlogPosition = binlogPosition;
        binlogDumpCmd.slaveServerId = this.slaveId;
        byte[] cmdBody = binlogDumpCmd.toBytes();

        logger.info("COM_BINLOG_DUMP with position:{}", binlogDumpCmd);
        HeaderPacket binlogDumpHeader = new HeaderPacket();
        binlogDumpHeader.setPacketBodyLength(cmdBody.length);
        binlogDumpHeader.setPacketSequenceNumber((byte) 0x00);
        PacketManager.writePkg(connector.getChannel(), binlogDumpHeader.toBytes(), cmdBody);
        connector.setDumping(true);
    }

    public void sendSemiAck(String binlogfilename, Long binlogPosition) throws IOException {
        SemiAckCommandPacket semiAckCmd = new SemiAckCommandPacket();
        semiAckCmd.binlogFileName = binlogfilename;
        semiAckCmd.binlogPosition = binlogPosition;

        byte[] cmdBody = semiAckCmd.toBytes();

        logger.info("SEMI ACK with position:{}", semiAckCmd);
        HeaderPacket semiAckHeader = new HeaderPacket();
        semiAckHeader.setPacketBodyLength(cmdBody.length);
        semiAckHeader.setPacketSequenceNumber((byte) 0x00);
        PacketManager.writePkg(connector.getChannel(), semiAckHeader.toBytes(), cmdBody);
    }

    private void sendBinlogDumpGTID(MysqlGTIDSet gtidSet) throws IOException {
        sendMySQLBinlogDumpGTID(gtidSet);
    }

    private void sendMySQLBinlogDumpGTID(MysqlGTIDSet gtidSet) throws IOException {
        BinlogDumpGTIDCommandPacket binlogDumpCmd = new BinlogDumpGTIDCommandPacket();
        binlogDumpCmd.slaveServerId = this.slaveId;
        binlogDumpCmd.gtidSet = gtidSet;
        byte[] cmdBody = binlogDumpCmd.toBytes();

        logger.info("COM_BINLOG_DUMP_GTID:{}", binlogDumpCmd);
        HeaderPacket binlogDumpHeader = new HeaderPacket();
        binlogDumpHeader.setPacketBodyLength(cmdBody.length);
        binlogDumpHeader.setPacketSequenceNumber((byte) 0x00);
        PacketManager.writePkg(connector.getChannel(), binlogDumpHeader.toBytes(), cmdBody);
        connector.setDumping(true);
    }

    public long queryServerId() throws IOException {
        ResultSetPacket resultSetPacket = query("show variables like 'server_id'");
        List<String> fieldValues = resultSetPacket.getFieldValues();
        if (fieldValues == null || fieldValues.size() != 2) {
            return 0;
        }
        return NumberUtils.toLong(fieldValues.get(1));
    }

    // ====================== help method ====================

    /**
     * the settings that will need to be checked or set:<br>
     * <ol>
     * <li>wait_timeout</li>
     * <li>net_write_timeout</li>
     * <li>net_read_timeout</li>
     * </ol>
     * 
     * @throws IOException
     */
    private void updateSettings() throws IOException {
        try {
            update("set wait_timeout=9999999");
        } catch (Exception e) {
            logger.warn("update wait_timeout failed", e);
        }
        try {
            update("set net_write_timeout=7200");
        } catch (Exception e) {
            logger.warn("update net_write_timeout failed", e);
        }

        try {
            update("set net_read_timeout=7200");
        } catch (Exception e) {
            logger.warn("update net_read_timeout failed", e);
        }

        try {
            // 设置服务端返回结果时不做编码转化，直接按照数据库的二进制编码进行发送，由客户端自己根据需求进行编码转化
            update("set names 'binary'");
        } catch (Exception e) {
            logger.warn("update names failed", e);
        }

        try {
            // mysql5.6针对checksum支持需要设置session变量
            // 如果不设置会出现错误： Slave can not handle replication events with the
            // checksum that master is configured to log
            // 但也不能乱设置，需要和mysql server的checksum配置一致，不然RotateLogEvent会出现乱码
            // '@@global.binlog_checksum'需要去掉单引号,在mysql 5.6.29下导致master退出
            update("set @master_binlog_checksum= @@global.binlog_checksum");
        } catch (Exception e) {
            if (!StringUtils.contains(e.getMessage(), "Unknown system variable")) {
                logger.warn("update master_binlog_checksum failed", e);
            }
        }

        try {
            // 参考:https://github.com/alibaba/canal/issues/284
            // mysql5.6需要设置slave_uuid避免被server kill链接
            update("set @slave_uuid=uuid()");
        } catch (Exception e) {
            if (!StringUtils.contains(e.getMessage(), "Unknown system variable")
                && !StringUtils.contains(e.getMessage(), "slave_uuid can't be set")) {
                logger.warn("update slave_uuid failed", e);
            }
        }

        /**
         * MASTER_HEARTBEAT_PERIOD sets the interval in seconds between
         * replication heartbeats. Whenever the master's binary log is updated
         * with an event, the waiting period for the next heartbeat is reset.
         * interval is a decimal value having the range 0 to 4294967 seconds and
         * a resolution in milliseconds; the smallest nonzero value is 0.001.
         * Heartbeats are sent by the master only if there are no unsent events
         * in the binary log file for a period longer than interval.
         */
        try {
            long periodNano = TimeUnit.SECONDS.toNanos(MASTER_HEARTBEAT_PERIOD_SECONDS);
            update("SET @master_heartbeat_period=" + periodNano);
        } catch (Exception e) {
            logger.warn("update master_heartbeat_period failed", e);
        }
    }

    /**
     * 获取一下binlog format格式
     */
    private void loadBinlogFormat() {
        ResultSetPacket rs = null;
        try {
            rs = query("show variables like 'binlog_format'");
        } catch (IOException e) {
            throw new WaterGapException("load binlog format error", e);
        }

        List<String> columnValues = rs.getFieldValues();
        if (columnValues == null || columnValues.size() != 2) {
            logger.warn("unexpected binlog format query result, this may cause unexpected result, so throw exception to request network to io shutdown.");
            throw new IllegalStateException("unexpected binlog format query result:" + rs.getFieldValues());
        }

        binlogFormat = BinlogFormat.valuesOf(columnValues.get(1));
        if (binlogFormat == null) {
            throw new IllegalStateException("unexpected binlog format query result:" + rs.getFieldValues());
        }
    }

    /**
     * 获取一下binlog image格式
     */
    private void loadBinlogImage() {
        ResultSetPacket rs = null;
        try {
            rs = query("show variables like 'binlog_row_image'");
        } catch (IOException e) {
            throw new WaterGapException("load binlog image error", e);
        }

        List<String> columnValues = rs.getFieldValues();
        if (columnValues == null || columnValues.size() != 2) {
            // 可能历时版本没有image特性
            binlogImage = BinlogImage.FULL;
        } else {
            binlogImage = BinlogImage.valuesOf(columnValues.get(1));
        }

        if (binlogFormat == null) {
            throw new IllegalStateException("unexpected binlog image query result:" + rs.getFieldValues());
        }
    }

    /**
     * 获取主库checksum信息
     * 
     * <pre>
     * mariadb区别于mysql会在binlog的第一个事件Rotate_Event里也会采用checksum逻辑,而mysql是在第二个binlog事件之后才感知是否需要处理checksum
     * 导致maraidb只要是开启checksum就会出现binlog文件名解析乱码
     * fixed issue : https://github.com/alibaba/canal/issues/1081
     * </pre>
     */
    private void loadBinlogChecksum() {
        ResultSetPacket rs = null;
        try {
            rs = query("select @@global.binlog_checksum");
            List<String> columnValues = rs.getFieldValues();
            if (columnValues != null && columnValues.size() >= 1 && columnValues.get(0) != null
                && columnValues.get(0).toUpperCase().equals("CRC32")) {
                binlogChecksum = BaseLogEvent.BINLOG_CHECKSUM_ALG_CRC32;
            } else {
                binlogChecksum = BaseLogEvent.BINLOG_CHECKSUM_ALG_OFF;
            }
        } catch (Throwable e) {
            // logger.error("", e);
            binlogChecksum = BaseLogEvent.BINLOG_CHECKSUM_ALG_OFF;
        }
    }

    private void accumulateReceivedBytes(long x) {
        if (receivedBinlogBytes != null) {
            receivedBinlogBytes.addAndGet(x);
        }
    }

    public static enum BinlogFormat {

        STATEMENT("STATEMENT"), ROW("ROW"), MIXED("MIXED");

        public boolean isStatement() {
            return this == STATEMENT;
        }

        public boolean isRow() {
            return this == ROW;
        }

        public boolean isMixed() {
            return this == MIXED;
        }

        private String value;

        private BinlogFormat(String value){
            this.value = value;
        }

        public static BinlogFormat valuesOf(String value) {
            BinlogFormat[] formats = values();
            for (BinlogFormat format : formats) {
                if (format.value.equalsIgnoreCase(value)) {
                    return format;
                }
            }
            return null;
        }
    }

    /**
     * http://dev.mysql.com/doc/refman/5.6/en/replication-options-binary-log.
     * html#sysvar_binlog_row_image
     * 
     * @author agapple 2015年6月29日 下午10:39:03
     * @since 1.0.20
     */
    public static enum BinlogImage {

        FULL("FULL"), MINIMAL("MINIMAL"), NOBLOB("NOBLOB");

        public boolean isFull() {
            return this == FULL;
        }

        public boolean isMinimal() {
            return this == MINIMAL;
        }

        public boolean isNoBlob() {
            return this == NOBLOB;
        }

        private String value;

        private BinlogImage(String value){
            this.value = value;
        }

        public static BinlogImage valuesOf(String value) {
            BinlogImage[] formats = values();
            for (BinlogImage format : formats) {
                if (format.value.equalsIgnoreCase(value)) {
                    return format;
                }
            }
            return null;
        }
    }

    // ================== setter / getter ===================

    public Charset getCharset() {
        return charset;
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    public long getSlaveId() {
        return slaveId;
    }

    public void setSlaveId(long slaveId) {
        this.slaveId = slaveId;
    }

    public MysqlConnector getConnector() {
        return connector;
    }

    public void setConnector(MysqlConnector connector) {
        this.connector = connector;
    }

    public BinlogFormat getBinlogFormat() {
        if (binlogFormat == null) {
            synchronized (this) {
                loadBinlogFormat();
            }
        }

        return binlogFormat;
    }

    public BinlogImage getBinlogImage() {
        if (binlogImage == null) {
            synchronized (this) {
                loadBinlogImage();
            }
        }

        return binlogImage;
    }

    public void setConnTimeout(int connTimeout) {
        this.connTimeout = connTimeout;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }


    public void setReceivedBinlogBytes(AtomicLong receivedBinlogBytes) {
        this.receivedBinlogBytes = receivedBinlogBytes;
    }

}
