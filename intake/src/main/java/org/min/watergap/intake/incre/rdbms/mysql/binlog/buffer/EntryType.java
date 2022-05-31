package org.min.watergap.intake.incre.rdbms.mysql.binlog.buffer;

/**
 * buffer type
 *
 * @Create by metaX.h on 2022/5/22 23:21
 */
public enum EntryType {
    ENTRYTYPECOMPATIBLEPROTO2(0),
    TRANSACTIONBEGIN(1),
    ROWDATA(2),
    TRANSACTIONEND(3),
    /** 心跳类型，内部使用，外部暂不可见，可忽略 **/
    HEARTBEAT(4),
    GTIDLOG(5);

    private int type;
    private EntryType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }
}
