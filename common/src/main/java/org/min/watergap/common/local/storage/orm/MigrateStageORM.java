package org.min.watergap.common.local.storage.orm;

import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.table.DatabaseTable;

/**
 * 记录迁移状态
 * 开启迁移->schema迁移完成->表结构迁移完成->数据迁移中->全量完成
 *
 * @Create by metaX.h on 2022/4/5 15:29
 */
@DatabaseTable(tableName = "MIGRATE_STAGE")
public class MigrateStageORM {

    @DatabaseField(columnName = "id", generatedId = true)
    private Long id;

    @DatabaseField(columnName="stage")
    private String stage;

    public MigrateStageORM() {
    }

    public MigrateStageORM(String stage) {
        this.stage = stage;
    }

    public static enum StageEnum {
        START, SCHEMA_MIGRATED, TABLE_MIGRATED, DATA_MIGRATING, FULL_OVER;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getStage() {
        return stage;
    }

    public void setStage(String stage) {
        this.stage = stage;
    }
}
