package org.min.watergap.intake.full;

import org.min.watergap.common.piping.data.impl.TableDataBasePipingData;
import org.min.watergap.common.piping.struct.impl.TableStructBasePipingData;
import org.min.watergap.intake.DataPumper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * 数据导出pump，具有DBStruct执行能力
 *
 * @Create by metaX.h on 2022/3/20 20:07
 */
public abstract class DBDataPumper extends DBStructPumper implements DataPumper {

    protected abstract String generateSelectSQL(TableDataBasePipingData tableData);

    protected Object convertSqlType(TableStructBasePipingData.Column column, ResultSet resultSet) throws SQLException {

        switch (column.getColumnType()) {
            case Types.VARCHAR :
                resultSet.getString(column.getColumnName());
                break;
            case Types.ARRAY:
                break;


        }
    }
}
