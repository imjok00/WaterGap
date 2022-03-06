package org.min.watergap.outfall;

import org.min.watergap.common.context.WaterGapContext;
import org.min.watergap.common.piping.data.impl.BasePipingData;
import org.min.watergap.common.piping.data.impl.SchemaStructBasePipingData;
import org.min.watergap.outfall.convertor.ConvertorChooser;
import org.min.watergap.outfall.convertor.StructConvertor;
import org.min.watergap.outfall.rdbms.RdbmsDataExecutor;

/**
 * 关系型数据库执行器
 *
 * @Create by metaX.h on 2022/3/4 22:55
 */
public class RdbmsOutFallDrainer extends OutFallDrainer {


    protected void doExecute(BasePipingData dataStruct) {
        switch (dataStruct.getType()) {
            case SCHEMA:
                SchemaStructBasePipingData schemaStruct = (SchemaStructBasePipingData) dataStruct;
                StructConvertor structConvertor = ConvertorChooser.chooseConvertor(targetDBType);
                dataExecutor.execute(null, structConvertor.convert(schemaStruct));
                break;

        }
    }

    @Override
    public void init(WaterGapContext waterGapContext) {
        dataExecutor = new RdbmsDataExecutor();
        dataExecutor.init(waterGapContext);
        pollTimeout = waterGapContext.getGlobalConfig().getPollTimeout();
        structPiping = waterGapContext.getStructPiping();
        targetDBType = waterGapContext.getGlobalConfig().getTargetConfig().getDatabaseType();
    }

}
