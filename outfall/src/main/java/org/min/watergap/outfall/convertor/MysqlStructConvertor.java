package org.min.watergap.outfall.convertor;

import org.min.watergap.intake.full.rdbms.struct.BaseStruct;
import org.min.watergap.intake.full.rdbms.struct.SchemaStruct;

public class MysqlStructConvertor implements StructConvertor {

    private static final String CREATE_DATABASE_TEMPLATE = "CREATE DATABASE %s CHARACTER SET %s COLLATE %s";

    @Override
    public String convert(BaseStruct sourceStruct) {
        switch (sourceStruct.getType()) {
            case schema:
                SchemaStruct schemaStruct = (SchemaStruct) sourceStruct;
                return String.format(CREATE_DATABASE_TEMPLATE, schemaStruct.getSchemaName(),
                        schemaStruct.getCharset(), schemaStruct.getCollate());
            case table:
                break;
        }
        return null;
    }
}
