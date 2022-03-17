package org.min.watergap.outfall.convertor;

import org.min.watergap.common.piping.data.impl.BasePipingData;
import org.min.watergap.common.piping.data.impl.SchemaStructBasePipingData;
import org.min.watergap.common.piping.data.impl.TableStructBasePipingData;

public class MysqlStructConvertor implements StructConvertor {

    private static final String CREATE_DATABASE_TEMPLATE = "CREATE DATABASE %s";

    @Override
    public String convert(BasePipingData pipingData) {
        switch (pipingData.getType()) {
            case SCHEMA:
                SchemaStructBasePipingData schemaStruct = (SchemaStructBasePipingData) pipingData;
                return String.format(CREATE_DATABASE_TEMPLATE, schemaStruct.getName());
            case TABLE:
                TableStructBasePipingData tableStruct = (TableStructBasePipingData) pipingData;

                break;
        }
        return null;
    }
}
