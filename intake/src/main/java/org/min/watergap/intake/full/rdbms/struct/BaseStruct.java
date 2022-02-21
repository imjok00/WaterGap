package org.min.watergap.intake.full.rdbms.struct;

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
        schema, table, column, function, procedure, event, trigger;
    }

    public static enum migrateStatus {
        INIT(0), // 初始化
        FAIL(-1), // 失败
        SUCCESS(1); // 成功


        private int status;
        migrateStatus(int status) {
            this.status = status;
        }

        public int getStatus() {
            return status;
        }
    }
}
