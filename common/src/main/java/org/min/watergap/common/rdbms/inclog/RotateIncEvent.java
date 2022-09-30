package org.min.watergap.common.rdbms.inclog;

/**
 * 换页
 *
 * @Create by metaX.h on 2022/6/5 13:17
 */
public class RotateIncEvent implements IncEvent {

    private String          filename;
    private long            position;

    public RotateIncEvent(String filename, long position) {
        this.filename = filename;
        this.position = position;
    }


}
