package org.min.watergap.intake.full.rdbms;

import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.common.piping.data.impl.TableDataBasePipingData;
import org.min.watergap.common.position.Position;
import org.min.watergap.intake.full.DBDataPumper;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * 关系型数据库数据导出
 *
 * @Create by metaX.h on 2022/3/20 20:09
 */
public class RdbmsDataPumper extends DBDataPumper {

    @Override
    public void isStart() {

    }

    @Override
    public void persistPosition(Position position) {

    }

    @Override
    public void pump() throws WaterGapException {
        runPumpWork(() -> {
            for(;;) {
                try {
                    TableDataBasePipingData tableData = (TableDataBasePipingData) dataPiping.take();
                    String selectSql = generateSelectSQL(tableData);
                    executeStreamQuery(tableData.getSchemaName(), selectSql, (resultSet) -> {
                        TableDataBasePipingData.ColumnValContain contain
                                = new TableDataBasePipingData.ColumnValContain(tableData.getColumns());
                        Map<String, Object> map = new HashMap<>();
                        while (resultSet.next()) {
                            tableData.getColumns().forEach(column -> {
                                map.put(column.getColumnName(), )
                            });
                        }
                    });

                } catch (InterruptedException | SQLException e) {
                    break;
                }
            }
        });
    }



    @Override
    protected String generateSelectSQL(TableDataBasePipingData tableData) {
        return null;
    }
}
