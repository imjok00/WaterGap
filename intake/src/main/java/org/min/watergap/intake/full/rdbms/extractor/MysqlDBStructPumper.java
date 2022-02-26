package org.min.watergap.intake.full.rdbms.extractor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.common.piping.PipingData;
import org.min.watergap.common.piping.data.impl.SchemaStructBasePipingData;
import org.min.watergap.common.utils.CollectionsUtils;
import org.min.watergap.intake.full.rdbms.RdbmsDBStructPumper;
import org.min.watergap.intake.full.rdbms.local.LocalFullStatusSaver;

import java.sql.SQLException;
import java.util.List;

/**
 * mysql 全量数据导出类
 *
 * @Create by metaX.h on 2021/11/4 23:30
 */
public class MysqlDBStructPumper extends RdbmsDBStructPumper {
    private static final Logger LOG = LogManager.getLogger(MysqlDBStructPumper.class);

    @Override
    protected void extractDBSchema() throws WaterGapException {
        List<PipingData> tableStructs = null;
        try {
            tableStructs = getAllSchemaStructs();
        } catch (Exception e) {
            throw new WaterGapException("show table struct error", e);
        }
        if (CollectionsUtils.isNotEmpty(tableStructs)) {
            tableStructs.forEach(schemaStruct -> {
                try {
                    LocalFullStatusSaver.save(schemaStruct);
                    structPiping.put(schemaStruct);
                } catch (SQLException e) {
                    throw new WaterGapException("save FullSchemaStatus to local fail", e);
                } catch (InterruptedException e) {
                    throw new WaterGapException("put schemaStruct to fsink fail", e);
                }
            });
        }
    }

    @Override
    protected void extractTableStructs(SchemaStructBasePipingData pipingData) throws WaterGapException {
        try {
            structPiping.poll(pollTimeout);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void destroy() {

    }
}
