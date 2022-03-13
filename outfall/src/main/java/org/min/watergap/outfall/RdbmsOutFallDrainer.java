package org.min.watergap.outfall;

import org.min.watergap.common.context.WaterGapContext;
import org.min.watergap.common.local.storage.LocalDataSaveTool;
import org.min.watergap.common.local.storage.entity.AbstractLocalStorageEntity;
import org.min.watergap.common.piping.data.impl.BasePipingData;
import org.min.watergap.common.piping.data.impl.SchemaStructBasePipingData;
import org.min.watergap.common.piping.data.impl.TableStructBasePipingData;
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
        StructConvertor structConvertor = ConvertorChooser.chooseConvertor(targetDBType);;
        switch (dataStruct.getType()) {
            case SCHEMA:
                SchemaStructBasePipingData schemaStruct = (SchemaStructBasePipingData) dataStruct;
                dataExecutor.execute(null, structConvertor.convert(schemaStruct), () -> {
                    LocalDataSaveTool.updateLocalDataStatus(dataStruct, AbstractLocalStorageEntity.LocalStorageStatus.COMPLETE.getStatus());
                    ack(dataStruct);
                });
                break;
            case TABLE:
                TableStructBasePipingData tableStruct = (TableStructBasePipingData) dataStruct;
                dataExecutor.execute(tableStruct.)
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
        ackPiping = waterGapContext.getAckPiping();
    }

}
