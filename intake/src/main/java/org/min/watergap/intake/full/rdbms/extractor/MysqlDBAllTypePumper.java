package org.min.watergap.intake.full.rdbms.extractor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.position.Position;
import org.min.watergap.common.rdbms.struct.StructType;
import org.min.watergap.common.utils.CollectionsUtils;
import org.min.watergap.intake.full.rdbms.RdbmsDBStructPumper;
import org.min.watergap.piping.translator.PipingData;
import org.min.watergap.piping.translator.impl.FullTableDataPipingData;
import org.min.watergap.piping.translator.impl.TableStructBasePipingData;
import org.min.watergap.piping.translator.impl.SchemaStructBasePipingData;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * mysql 全量数据导出类
 *
 * @Create by metaX.h on 2021/11/4 23:30
 */
public class MysqlDBAllTypePumper extends RdbmsDBStructPumper {
    private static final Logger LOG = LogManager.getLogger(MysqlDBAllTypePumper.class);
    public static final List<String> SYSTEM_EXCLUDE_SCHEMAS = Arrays.asList("information_schema", "mysql", "performance_schema", "sys");

    @Override
    protected List<PipingData> filterPipData(List<PipingData> pipingDataList) {
        return pipingDataList.stream().filter(pipingData ->
                StructType.SCHEMA.equals(pipingData.getType())
                        && !SYSTEM_EXCLUDE_SCHEMAS.contains(((SchemaStructBasePipingData) pipingData).getSchemaName())
        ).collect(Collectors.toList());
    }


    @Override
    protected String generateSelectSQL(FullTableDataPipingData tableData) {
        StringBuilder selectSQL = new StringBuilder("SELECT ");
        selectSQL.append(tableData.getColumns().stream()
                .map(TableStructBasePipingData.Column::getColumnName)
                .collect(Collectors.joining(",")));
        selectSQL.append(" FROM ").append(tableData.getTableName());

        if (tableData.getIndexInfo() != null
                && CollectionsUtils.isNotEmpty(tableData.getIndexInfo().getPrimaryKeys())
                && !tableData.getPosition().isFirst()) {
            selectSQL.append(generateSearchKeys(tableData.getIndexInfo().getPrimaryKeys(), tableData.getPosition()));
            selectSQL.append(" LIMIT ").append(sqlSelectLimit);
        }

        return selectSQL.toString();
    }

    private String generateSearchKeys(List<TableStructBasePipingData.Column> list, Position position) {
        StringBuilder whereSQL = new StringBuilder().append(" WHERE ");
        StringBuilder orderBy = new StringBuilder(" ORDER BY ");
        for (TableStructBasePipingData.Column column : list) {
            if (column.isFullKey()) {
                whereSQL.append(column.getColumnName()).append(">").append(position.getVal());
                orderBy.append(column.getColumnName()).append(" ASC");
                break;
            }
        }
        return whereSQL.append(orderBy).toString();

    }

    protected void extractTableOption(TableStructBasePipingData pipingData) throws SQLException, InterruptedException {
        executeQuery("information_schema", "SELECT * from tables where TABLE_NAME = '" + pipingData.getTableName() + "'", resultSet->{
            while (resultSet.next()) {
                pipingData.addTableOption(TableStructBasePipingData.TABLE_OPTION_COMMENT, resultSet.getString("TABLE_COMMENT"));
            }
        });
    }

    @Override
    public void destroy() {
        super.destroy();
    }

}
