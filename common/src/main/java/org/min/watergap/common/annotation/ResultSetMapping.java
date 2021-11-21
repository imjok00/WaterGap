package org.min.watergap.common.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 用于设置ResultSet映射关系，方便提取数据
 *S
 * @Create by metaX.h on 2021/11/21 21:31
 */
@Target(value = ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ResultSetMapping {

    // ResultSet获取的列名
    public String value();

}
