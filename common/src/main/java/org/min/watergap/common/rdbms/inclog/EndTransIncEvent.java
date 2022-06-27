package org.min.watergap.common.rdbms.inclog;

/**
 * 事务结束
 *
 * @Create by metaX.h on 2022/6/5 13:17
 */
public class EndTransIncEvent implements IncEvent {

    private long xid;
    public EndTransIncEvent() {
    }

    public EndTransIncEvent(long xid) {
        this.xid = xid;
    }

}
