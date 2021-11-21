package org.min.watergap.intake.full.rdbms.to;

/**
 * 基础数据类型
 *
 * @Create by metaX.h on 2021/11/14 19:13
 */
public class BaseStruct {

    /**
     * 数据类型
     */
    private BaseType type;

    public BaseType getType() {
        return type;
    }

    public void setType(BaseType type) {
        this.type = type;
    }

    public static enum BaseType {
        table, column, function, procedure, event, trigger;
    }
}
