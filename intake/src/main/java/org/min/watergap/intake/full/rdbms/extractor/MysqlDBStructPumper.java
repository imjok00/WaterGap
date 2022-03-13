package org.min.watergap.intake.full.rdbms.extractor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.min.watergap.common.piping.PipingData;
import org.min.watergap.common.piping.data.impl.SchemaStructBasePipingData;
import org.min.watergap.common.rdbms.struct.StructType;
import org.min.watergap.intake.full.rdbms.RdbmsDBStructPumper;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * mysql 全量数据导出类
 *
 * @Create by metaX.h on 2021/11/4 23:30
 */
public class MysqlDBStructPumper extends RdbmsDBStructPumper {
    private static final Logger LOG = LogManager.getLogger(MysqlDBStructPumper.class);
    public static final List<String> SYSTEM_EXCLUDE_SCHEMAS = Arrays.asList("information_schema", "mysql", "performance_schema", "sys");

    @Override
    protected List<PipingData> filterPipData(List<PipingData> pipingDataList) {
        return pipingDataList.stream().filter(pipingData ->
                StructType.SCHEMA.equals(pipingData.getType())
                        && SYSTEM_EXCLUDE_SCHEMAS.contains(((SchemaStructBasePipingData) pipingData).getName())
        ).collect(Collectors.toList());
    }

    @Override
    public void destroy() {

    }

    @Override
    public void isStart() {

    }
}
