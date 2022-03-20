package org.min.watergap.intake.full.rdbms;

import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.common.piping.data.impl.TableDataBasePipingData;
import org.min.watergap.common.position.Position;
import org.min.watergap.intake.full.DBDataPumper;

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



                } catch (InterruptedException e) {
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
