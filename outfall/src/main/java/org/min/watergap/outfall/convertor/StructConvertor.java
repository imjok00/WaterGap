package org.min.watergap.outfall.convertor;

import org.min.watergap.common.piping.struct.impl.BasePipingData;

/**
 * 结构转换器，主要有schema，table转换
 */
public interface StructConvertor {

    String convert(BasePipingData pipingData);

}
