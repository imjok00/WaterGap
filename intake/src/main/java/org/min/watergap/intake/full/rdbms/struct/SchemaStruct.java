package org.min.watergap.intake.full.rdbms.struct;

import org.min.watergap.common.local.storage.entity.FullSchemaStatus;

/**
 * schema结构信息
 *
 * @Create by metaX.h on 2021/11/28 23:22
 */
public class SchemaStruct extends BaseStruct {

    private String schemaName;

    private String charset;

    private String collate;

    public SchemaStruct(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public String getCollate() {
        return collate;
    }

    public void setCollate(String collate) {
        this.collate = collate;
    }

    public static SchemaStruct newObject(String shcemaName) {
        return new SchemaStruct(shcemaName);
    }

    /**
     * 将数据对象转换成中间存储对象
     * @param schemaStruct
     * @return
     */
    public static FullSchemaStatus convet(SchemaStruct schemaStruct) {
        FullSchemaStatus fullSchemaStatus = new FullSchemaStatus();
        fullSchemaStatus.setSchemaName(schemaStruct.getSchemaName());
        fullSchemaStatus.setStatus(migrateStatus.INIT.getStatus());
        return fullSchemaStatus;
    }
}
