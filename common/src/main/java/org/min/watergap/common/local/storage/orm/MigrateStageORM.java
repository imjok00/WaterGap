package org.min.watergap.common.local.storage.orm;

/**
 * 记录迁移状态
 * 开启迁移->schema迁移完成->表结构迁移完成->数据迁移中->全量完成
 *
 * @Create by metaX.h on 2022/4/5 15:29
 */
public class MigrateStageORM {

    private String stage;

    public MigrateStageORM() {
    }

    public MigrateStageORM(String stage) {
        this.stage = stage;
    }

    public static enum StageEnum {
        START, SCHEMA_MIGRATED, TABLE_MIGRATED, DATA_MIGRATING, FULL_OVER;
    }

    public String getStage() {
        return stage;
    }

    public void setStage(String stage) {
        this.stage = stage;
    }
}
