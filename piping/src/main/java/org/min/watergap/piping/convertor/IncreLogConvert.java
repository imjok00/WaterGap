package org.min.watergap.piping.convertor;

import org.min.watergap.common.rdbms.misc.binlog.BaseLogEvent;
import org.min.watergap.piping.translator.PipingEvent;

/**
 * 增量数据转换
 *
 * @Create by metaX.h on 2022/5/29 20:56
 */
public interface IncreLogConvert {

    PipingEvent convert(BaseLogEvent event);

}
