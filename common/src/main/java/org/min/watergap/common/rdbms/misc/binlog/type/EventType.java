package org.min.watergap.common.rdbms.misc.binlog.type;

/**
 * Todo
 *
 * @Create by metaX.h on 2022/5/23 0:18
 */
public enum EventType {

    EVENTTYPECOMPATIBLEPROTO2(0),
    INSERT(1),
    UPDATE(2),
    DELETE(3),
    CREATE(4),
    ALTER(5),
    ERASE(6),
    QUERY(7),
    TRUNCATE(8),
    RENAME(9),
    /**CREATE INDEX**/
    CINDEX(10),
    DINDEX(11),
    GTID(12),
    /** XA **/
    XACOMMIT(13),
    XAROLLBACK(14),
    /** MASTER HEARTBEAT **/
    MHEARTBEAT(15);


    private int type;
    private EventType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }
}
