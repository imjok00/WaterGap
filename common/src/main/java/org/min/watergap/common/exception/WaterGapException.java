package org.min.watergap.common.exception;

import org.min.watergap.common.enums.PumpExceptionEnum;

/**
 * 统一异常处理
 *
 * @Create by metaX.h on 2021/11/7 10:45
 */
public class WaterGapException extends RuntimeException {

    private int code;

    public WaterGapException(PumpExceptionEnum exceptionEnum) {
        super(exceptionEnum.getMsg());
        code = exceptionEnum.getCode();
    }

    public WaterGapException(String errorCode, String errorMsg) {
        super(errorCode + ":" + errorMsg);
    }

    public WaterGapException(String errorMsg, Throwable e) {
        super(errorMsg, e);
    }

    public WaterGapException(String errorCode, String errorMsg, Throwable e) {
        super(errorCode + ":" + errorMsg, e);
    }

}
