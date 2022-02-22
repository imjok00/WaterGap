package org.min.watergap.intake.full.rdbms.extractor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.config.WaterGapGlobalConfig;
import org.min.watergap.common.datasource.DataSourceFactory;
import org.min.watergap.common.exception.WaterGapException;
import org.min.watergap.common.local.storage.entity.FullSchemaStatus;
import org.min.watergap.common.utils.CollectionsUtils;
import org.min.watergap.intake.dialect.DBDialectWrapper;
import org.min.watergap.intake.full.rdbms.RdbmsDBStructPumper;
import org.min.watergap.intake.full.rdbms.local.LocalFullStatusSaver;
import org.min.watergap.intake.full.rdbms.struct.SchemaStruct;

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
    protected void extractTableSchema() throws WaterGapException {
        List<SchemaStruct> tableStructs = null;
        try {
            tableStructs = getAllSchemaStructs();
        } catch (Exception e) {
            throw new WaterGapException("show table struct error", e);
        }
        if (CollectionsUtils.isNotEmpty(tableStructs)) {
            tableStructs.forEach(schemaStruct -> {
                try {
                    FullSchemaStatus fullSchemaStatus = SchemaStruct.convet(schemaStruct);
                    LocalFullStatusSaver.save(fullSchemaStatus);
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
    protected void extractTableStructs() throws WaterGapException {

    }

    @Override
    protected void extractTableDatas() {

    }

    @Override
    public void init(WaterGapGlobalConfig config) {
        super.init(config);
        this.baseConfig = config;
        this.dataSource = DataSourceFactory.getDataSource(config.getSourceConfig());
        this.pumperDBDialect = new DBDialectWrapper(config.getSourceConfig().getDatabaseType());
    }

    @Override
    public void destroy() {

    }
}
