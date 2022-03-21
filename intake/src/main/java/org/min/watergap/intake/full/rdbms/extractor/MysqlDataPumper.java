package org.min.watergap.intake.full.rdbms.extractor;

import org.min.watergap.common.piping.data.impl.TableDataBasePipingData;
import org.min.watergap.common.piping.struct.impl.TableStructBasePipingData;
import org.min.watergap.common.position.Position;
import org.min.watergap.common.position.full.RdbmsFullPosition;
import org.min.watergap.intake.full.rdbms.RdbmsDataPumper;

import java.util.List;

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
            selectSQL.append(generateSearchKeys(tableData.getColumns(), tableData.getPosition()));
        }
        return selectSQL.toString();
    }

    private String generateSearchKeys(List<TableStructBasePipingData.Column> list, Position position) {
        if (((RdbmsFullPosition)position).isStartPosition()) {
            StringBuilder orderBy = new StringBuilder(" ORDER BY ");
            for (int i = 0, length = list.size(); i < length; i++) {
                if (i == length - 1) {
                    orderBy.append(list.get(i).getColumnName()).append(" ASC");
                } else {
                    orderBy.append(list.get(i).getColumnName()).append(" ASC,");
                }
            }
            return orderBy.toString();
        } else {
            StringBuilder whereSQL = new StringBuilder().append(" WHERE ");
            StringBuilder orderBy = new StringBuilder(" ORDER BY ");
            for (int i = 0, length = list.size(); i < length; i++) {
                TableStructBasePipingData.Column column = list.get(i);
                whereSQL.append(column.getColumnName()).append(">").append(position.getVal(column.getColumnName()));

                if (i == length - 1) {
                    orderBy.append(list.get(i).getColumnName()).append(" ASC");
                } else {
                    whereSQL.append(" AND ");
                    orderBy.append(list.get(i).getColumnName()).append(" ASC,");
                }
            }
            return whereSQL.append(orderBy).toString();
        }

    }
}
