package org.min.watergap.intake.full.rdbms.struct;

/**
 * schema结构信息
 *
 * @Create by metaX.h on 2021/11/28 23:22
 */
public class SchemaStruct extends BaseStruct {

    private String schemaName;

    public SchemaStruct(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public static SchemaStruct newObject(String shcemaName) {
        return new SchemaStruct(shcemaName);
    }
}
