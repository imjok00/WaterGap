package org.min.watergap.intake.full.rdbms.ins.mysql;

import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.common.utils.CollectionsUtils;
import org.min.watergap.intake.full.rdbms.RdbmsFullExtractor;
import org.min.watergap.intake.full.rdbms.to.TableStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * mysql 全量数据导出类
 *
 * @Create by metaX.h on 2021/11/4 23:30
 */
public class MysqlFullExtractor extends RdbmsFullExtractor {
    private static final Logger LOG = LoggerFactory.getLogger(MysqlFullExtractor.class);

    @Override
    protected void extractTableSchema() throws WaterGapException {
        List<TableStruct> tableStructs = null;
        try {
            tableStructs = getAllTableStruct();
        } catch (Exception e) {
            throw new WaterGapException("show table struct error", e);
        }
        if (CollectionsUtils.isEmpty(tableStructs)) {
            LOG.warn("can not found table to intake");
        }
        tableStructs.forEach(tableStruct -> {

        });

    }

    @Override
    protected void extractTableDatas() {

    }
}
