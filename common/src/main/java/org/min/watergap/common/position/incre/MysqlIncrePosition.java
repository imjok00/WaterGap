package org.min.watergap.common.position.incre;

import com.google.gson.Gson;
import org.min.watergap.common.position.Position;

/**
 * mysql 位点记录
 *
 * @Create by metaX.h on 2022/4/30 10:33
 */
public class MysqlIncrePosition implements Position {

    private boolean isGtidMode;

    private String file;
    private Long position;

    private GTIDSet uuidSet;

    public MysqlIncrePosition() {}

    public MysqlIncrePosition(String file, Long position) {
        this.isGtidMode = false;
        this.file = file;
        this.position = position;
    }

    public MysqlIncrePosition(String file, String uuidset) {
        this.isGtidMode = true;
        this.file = file;
        this.uuidSet = MysqlGTIDSet.parse(uuidset);
    }

    public MysqlIncrePosition(String uuidset) {
        this.isGtidMode = true;
        this.file = file;
        this.uuidSet = MysqlGTIDSet.parse(uuidset);
    }

    @Override
    public String getVal() {
        return toString();
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    @Override
    public void parse(String position) {
        /**
         * mysql位点格式
         * 1: file : ppsition
         * 2: file : gtid : long
         */
        Gson gson = new Gson();
        MysqlIncrePosition mysqlPosition = gson.fromJson(position, MysqlIncrePosition.class);
        this.isGtidMode = mysqlPosition.isGtidMode;
        if (isGtidMode) {
            this.file = mysqlPosition.file;
            this.uuidSet = mysqlPosition.uuidSet;
        } else {
            this.file = mysqlPosition.file;
            this.position = mysqlPosition.position;
        }
    }

    @Override
    public boolean isFirst() {
        return false;
    }

    public boolean isGtidMode() {
        return isGtidMode;
    }

    public void setGtidMode(boolean gtidMode) {
        isGtidMode = gtidMode;
    }

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public Long getPosition() {
        return position;
    }

    public void setPosition(Long position) {
        this.position = position;
    }

    public GTIDSet getUuidSet() {
        return uuidSet;
    }

}
