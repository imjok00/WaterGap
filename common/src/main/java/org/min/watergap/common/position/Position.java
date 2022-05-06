package org.min.watergap.common.position;

/**
 * 用来记录同步的位置
 *
 * @Create by metaX.h on 2022/3/20 18:36
 */
public interface Position {

    String getVal();

    void parse(String position);

    boolean isFirst();

}
