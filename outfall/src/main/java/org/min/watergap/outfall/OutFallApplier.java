package org.min.watergap.outfall;

import org.min.watergap.common.config.DatabaseType;
import org.min.watergap.common.context.WaterGapContext;
import org.min.watergap.common.lifecycle.WaterGapLifeCycle;
import org.min.watergap.common.piping.PipingData;
import org.min.watergap.common.piping.StructPiping;
import org.min.watergap.common.piping.data.impl.BasePipingData;
import org.min.watergap.common.piping.data.impl.SchemaStructBasePipingData;
import org.min.watergap.outfall.convertor.ConvertorChooser;
import org.min.watergap.outfall.convertor.StructConvertor;
import org.min.watergap.outfall.rdbms.DataExecutor;
import org.min.watergap.outfall.rdbms.RdbmsDataExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutFallApplier implements WaterGapLifeCycle {
    private static final Logger LOG = LoggerFactory.getLogger(OutFallApplier.class);

    private DataExecutor dataExecutor;

    private StructPiping structPiping;

    private Long pollTimeout;

    private DatabaseType targetDBType;

    public void flowing() {
        try {
            PipingData pipingData = structPiping.poll(pollTimeout);
            doExecute((BasePipingData) pipingData);
        } catch (InterruptedException e) {
            LOG.error("poll event from fsink fail", e);
        }



    }

    private void doExecute(BasePipingData dataStruct) {
        switch (dataStruct.getType()) {
            case SCHEMA:
                SchemaStructBasePipingData schemaStruct = (SchemaStructBasePipingData) dataStruct;
                StructConvertor structConvertor = ConvertorChooser.chooseConvertor(targetDBType);
                dataExecutor.execute(schemaStruct.getName(),structConvertor.convert(schemaStruct));
                break;

        }
    }

    @Override
    public void init(WaterGapContext waterGapContext) {
        dataExecutor = new RdbmsDataExecutor();
        dataExecutor.init(waterGapContext);
        pollTimeout = waterGapContext.getGlobalConfig().getPollTimeout();
    }

    @Override
    public void destroy() {
        dataExecutor.destroy();
    }
}