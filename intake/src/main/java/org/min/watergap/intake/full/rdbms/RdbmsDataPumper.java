package org.min.watergap.intake.full.rdbms;

import org.min.watergap.common.context.WaterGapContext;
import org.min.watergap.common.local.storage.orm.MigrateStageORM;
import org.min.watergap.common.local.storage.orm.service.FullTableDataPositionService;
import org.min.watergap.common.local.storage.orm.service.FullTableStatusService;
import org.min.watergap.common.local.storage.orm.service.MigrateStageService;
import org.min.watergap.common.local.storage.orm.service.SchemaStatusService;
import org.min.watergap.common.utils.StringUtils;
import org.min.watergap.common.utils.ThreadLocalUtils;
import org.min.watergap.intake.full.DBPumper;
import org.min.watergap.piping.thread.SingleThreadWorkGroup;
import org.min.watergap.piping.translator.WaterGapPiping;
import org.min.watergap.piping.translator.impl.FullTableDataBasePipingData;
import org.min.watergap.piping.translator.impl.TableStructBasePipingData;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * 关系型数据库数据导出
 *
 * @Create by metaX.h on 2022/3/20 20:09
 */
public abstract class RdbmsDataPumper extends DBPumper {

    protected WaterGapPiping pumpPiping;
    protected WaterGapPiping ackPiping;
    protected SingleThreadWorkGroup[] workGroups;

    @Override
    public void init(WaterGapContext waterGapContext) {
        super.init(waterGapContext);
        initLocalService();
        workGroups = new SingleThreadWorkGroup[waterGapContext.getGlobalConfig().getExecutorWorkNum()];
    }

    private void initLocalService() {
        ThreadLocalUtils.set(FullTableStatusService.class.getName(), new FullTableStatusService());
        ThreadLocalUtils.set(FullTableDataPositionService.class.getName(), new FullTableDataPositionService());
        ThreadLocalUtils.set(SchemaStatusService.class.getName(), new SchemaStatusService());
        ThreadLocalUtils.set(MigrateStageService.class.getName(), new MigrateStageService());
    }

    public void injectPiping(WaterGapPiping pumpPiping, WaterGapPiping ackPiping) {
        this.pumpPiping = pumpPiping;
        this.ackPiping = ackPiping;
    }

    @Override
    public void destroy() {
        super.destroy();
        if (workGroups != null) {
            for (SingleThreadWorkGroup singleThreadWorkGroup : workGroups) {
                singleThreadWorkGroup.destroy();
            }
        }
    }

    protected void startStage() {
        super.start();
        MigrateStageORM migrateStageORM = ThreadLocalUtils.getMigrateStageService().queryOne();
        if (migrateStageORM == null) {
            ThreadLocalUtils.getMigrateStageService().create();
        }
    }


    protected void startNextDataPumper(FullTableDataBasePipingData tableData) throws InterruptedException, SQLException {
        if (tableData.isNeedInit()) {
            tryUpdatePosition(tableData);
        }
        String selectSql = generateSelectSQL(tableData);
        executeStreamQuery(tableData.getSchemaName(), selectSql, (resultSet) -> {
            FullTableDataBasePipingData.ColumnValContain contain
                    = new FullTableDataBasePipingData.ColumnValContain(tableData.getColumns());
            while (resultSet.next()) {
                Map<String, Object> map = new HashMap<>();
                for (TableStructBasePipingData.Column column : tableData.getColumns()) {
                    map.put(column.getColumnName(), convertSqlType(column, resultSet));
                }
                contain.addVal(map);
            }
            if (contain.isEmpty()) {
                // 全量完成
                ThreadLocalUtils.getFullTableDataPositionService().finishDataFull(tableData.getSchemaName(), tableData.getTableName());
                tryClose();
            } else {
                tableData.setContain(contain);
                pumpPiping.put(tableData);
            }

        });
    }

    /**
     * position不应低于本地存储position
     */
    protected void tryUpdatePosition(FullTableDataBasePipingData tableData) throws SQLException {
        String positionStr = ThreadLocalUtils.getFullTableDataPositionService()
                .queryLastPosition(tableData.getSchemaName(), tableData.getTableName());
        if (StringUtils.isNotEmpty(positionStr)) {
            tableData.getPosition().parse(positionStr);
        }
    }

    protected boolean tryClose() throws SQLException {
        if (ThreadLocalUtils.getFullTableDataPositionService().isAllComplete()) {
            stop();
            return true;
        }
        return false;
    }

    protected Object convertSqlType(TableStructBasePipingData.Column column, ResultSet resultSet) throws SQLException {

        switch (column.getColumnType()) {
            case Types.VARCHAR :
            case Types.CHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.NCLOB:
                return resultSet.getString(column.getColumnName());
            case Types.BOOLEAN:
                return resultSet.getBoolean(column.getColumnName());
            case Types.ARRAY:
                return resultSet.getArray(column.getColumnName());
            case Types.BIGINT:
                return resultSet.getLong(column.getColumnName());
            case Types.BIT:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
                return resultSet.getInt(column.getColumnName());
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                return resultSet.getBytes(column.getColumnName());
            case Types.FLOAT:
                return resultSet.getFloat(column.getColumnName());
            case Types.REAL:
            case Types.DOUBLE:
                return resultSet.getDouble(column.getColumnName());
            case Types.NUMERIC:
            case Types.DECIMAL:
                return resultSet.getBigDecimal(column.getColumnName());
            case Types.DATE:
                return resultSet.getDate(column.getColumnName());
            case Types.TIME:
                return resultSet.getTime(column.getColumnName());
            case Types.TIMESTAMP:
                return resultSet.getTimestamp(column.getColumnName());
//            case Types.TIME_WITH_TIMEZONE:
//            case Types.TIMESTAMP_WITH_TIMEZONE:
//            case Types.NULL:
//            case Types.OTHER:
//            case Types.JAVA_OBJECT:
//            case Types.DISTINCT:
//            case Types.STRUCT:
//            case Types.REF:
//            case Types.DATALINK:
//            case Types.ROWID:
//            case Types.SQLXML:
//            case Types.REF_CURSOR:
//                return resultSet.getObject(column.getColumnName());
            default:
                return resultSet.getObject(column.getColumnName());
        }
    }

    protected abstract String generateSelectSQL(FullTableDataBasePipingData tableData);

}
