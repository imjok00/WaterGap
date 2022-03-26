package org.min.watergap.intake.full.rdbms;

import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.common.piping.data.impl.FullTableDataBasePipingData;
import org.min.watergap.common.piping.struct.impl.TableStructBasePipingData;
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
                    FullTableDataBasePipingData tableData = (FullTableDataBasePipingData) dataPiping.take();
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
                        tableData.setContain(contain);
                        dataPiping.put(tableData);
                    });

                } catch (InterruptedException | SQLException e) {
                    break;
                }
            }
        });
    }



    @Override
    protected String generateSelectSQL(FullTableDataBasePipingData tableData) {
        return null;
    }
}
