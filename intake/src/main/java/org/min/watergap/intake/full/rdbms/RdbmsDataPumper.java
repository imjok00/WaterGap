package org.min.watergap.intake.full.rdbms;

import org.min.watergap.common.piping.data.impl.FullTableDataBasePipingData;
import org.min.watergap.common.piping.struct.impl.TableStructBasePipingData;
import org.min.watergap.common.position.Position;
import org.min.watergap.intake.full.DBDataPumper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * 关系型数据库数据导出
 *
 * @Create by metaX.h on 2022/3/20 20:09
 */
public abstract class RdbmsDataPumper extends DBDataPumper {

    protected volatile boolean running = false;

    @Override
    public boolean isStart() {
        return running;
    }

    public void setRunning() {
        running = true;
    }

    public void setStop() {
        running = false;
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

    @Override
    protected String generateSelectSQL(FullTableDataBasePipingData tableData) {
        return null;
    }
}