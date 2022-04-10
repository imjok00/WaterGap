package org.min.watergap.common.enums;

/**
 * 用于记录导出步骤
 *
 * @Create by metaX.h on 2022/2/27 22:19
 */
public enum PumpExceptionEnum {

    DATABASE_EXISTS(2001001, "database exists");

    PumpExceptionEnum(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    private int code;
    private String msg;

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
