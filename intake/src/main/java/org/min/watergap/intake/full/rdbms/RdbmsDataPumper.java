package org.min.watergap.intake.full.rdbms;

import org.min.watergap.common.context.WaterGapContext;
import org.min.watergap.common.local.storage.orm.MigrateStageORM;
import org.min.watergap.common.local.storage.orm.service.FullTableStatusService;
import org.min.watergap.common.local.storage.orm.service.MigrateStageService;
import org.min.watergap.common.local.storage.orm.service.SchemaStatusService;
import org.min.watergap.common.piping.struct.impl.TableStructBasePipingData;
import org.min.watergap.common.position.Position;
import org.min.watergap.intake.full.DBDataPumper;
import org.min.watergap.outfall.Drainer;
import org.min.watergap.outfall.RdbmsOutFallDrainer;
import org.min.watergap.piping.WaterGapDisruptor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * 关系型数据库数据导出
 *
 * @Create by metaX.h on 2022/3/20 20:09
 */
public abstract class RdbmsDataPumper extends DBDataPumper {

    protected WaterGapDisruptor waterGapDisruptor;
    protected Drainer[] drainers;
    // 存储在本地的基础对象
    protected FullTableStatusService fullTableStatusService;
    protected SchemaStatusService schemaStatusService;
    protected MigrateStageService migrateStageService;

    @Override
    public void init(WaterGapContext waterGapContext) {
        super.init(waterGapContext);
        fullTableStatusService = new FullTableStatusService();
        schemaStatusService = new SchemaStatusService();
        migrateStageService = new MigrateStageService();
        drainers = new RdbmsOutFallDrainer[waterGapContext.getGlobalConfig().getExecutorWorkNum()];
        waterGapDisruptor = new WaterGapDisruptor(drainers);
    }

    protected void startStage() {
        MigrateStageORM migrateStageORM = migrateStageService.queryOne();
        if (migrateStageORM == null) {
            migrateStageService.create();
        }
    }

    @Override
    public void persistPosition(Position position) {

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


}
