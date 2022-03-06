package org.min.watergap.common.utils;

import java.util.Collections;

/**
 * 字符串操作类
 *
 * @Create by metaX.h on 2021/12/15 0:02
 */
public class StringUtils {

    /**
     * 相同字符串重复N次
     * @param seed
     * @param n
     * @return
     */
    public static String createRepeatedStr(String seed,int n) {
        return String.join("", Collections.nCopies(n, seed));
    }

    public static String createReplaceStr(String seed,int n) {
        return String.join(",", Collections.nCopies(n, seed));
    }
}
