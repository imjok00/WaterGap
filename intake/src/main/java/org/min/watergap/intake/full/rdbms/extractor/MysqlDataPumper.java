package org.min.watergap.intake.full.rdbms.extractor;

import org.min.watergap.common.piping.data.impl.TableDataBasePipingData;
import org.min.watergap.common.piping.struct.impl.TableStructBasePipingData;
import org.min.watergap.intake.full.rdbms.RdbmsDataPumper;

/**
 * MySQL 数据泵
 *
 * @Create by metaX.h on 2022/3/20 22:29
 */
public class MysqlDataPumper extends RdbmsDataPumper {

    @Override
    protected String generateSelectSQL(TableDataBasePipingData tableData) {
        StringBuilder selectSQL = new StringBuilder("SELECT");
        for (TableStructBasePipingData.Column column : tableData.getColumns()) {
            selectSQL.append(" ").append(column.getColumnName());
        }
        selectSQL.append(" FROM ").append(tableData.getTableName());

        if (tableData.getIndexInfo().getPrimaryKeys() != null) {
            selectSQL.append(" WHERE ");

        }

    }
}
