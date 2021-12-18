package org.min.watergap.intake.full.rdbms.extractor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.common.utils.CollectionsUtils;
import org.min.watergap.intake.full.rdbms.RdbmsFullExtractor;
import org.min.watergap.intake.full.rdbms.struct.SchemaStruct;

import java.util.List;

/**
 * mysql 全量数据导出类
 *
 * @Create by metaX.h on 2021/11/4 23:30
 */
public class MysqlFullExtractor extends RdbmsFullExtractor {
    private static final Logger LOG = LogManager.getLogger(MysqlFullExtractor.class);

    @Override
    protected void extractTableSchema() throws WaterGapException {
        List<SchemaStruct> tableStructs = null;
        try {
            tableStructs = getAllSchemaStructs();
        } catch (Exception e) {
            throw new WaterGapException("show table struct error", e);
        }
        if (CollectionsUtils.isNotEmpty(tableStructs)) {
            //FullSchemaStatus
            //localFileSaver.FullSchemaStatus
        }

    }

    @Override
    protected void extractTableDatas() {

    }
}
