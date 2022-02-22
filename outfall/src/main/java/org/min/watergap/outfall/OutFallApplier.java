package org.min.watergap.outfall;

import org.min.watergap.common.config.WaterGapGlobalConfig;
import org.min.watergap.intake.full.rdbms.local.LocalFullStatusSaver;
import org.min.watergap.intake.full.rdbms.struct.BaseStruct;
import org.min.watergap.intake.full.rdbms.struct.SchemaStruct;
import org.min.watergap.outfall.convertor.ConvertorChooser;
import org.min.watergap.outfall.convertor.StructConvertor;
import org.min.watergap.outfall.rdbms.DataExecutor;
import org.min.watergap.piping.convertor.pip.StructPiping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutFallApplier {
    private static final Logger LOG = LoggerFactory.getLogger(OutFallApplier.class);

    private StructPiping fullPiping;

    private DataExecutor dataExecutor;

    private LocalFullStatusSaver localFileSaver;

    private WaterGapGlobalConfig baseConfig;

    public void flowing() {
        try {
            BaseStruct dataStruct = fullPiping.poll(baseConfig.getPollTimeout());
            doExecute(dataStruct);
        } catch (InterruptedException e) {
            LOG.error("poll event from fsink fail", e);
        }



    }

    private void doExecute(BaseStruct dataStruct) {
        switch (dataStruct.getType()) {
            case schema:
                SchemaStruct schemaStruct = (SchemaStruct) dataStruct;
                StructConvertor structConvertor = ConvertorChooser.chooseConvertor(baseConfig.getTargetConfig().getDatabaseType());
                dataExecutor.execute(schemaStruct.getSchemaName(),structConvertor.convert(schemaStruct));
                break;

        }
    }
}