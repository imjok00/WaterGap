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

    public static boolean equalsIgnoreCase(String s1, String s2) {
        if (s1 == null) {
            return false;
        }
        return s1.equalsIgnoreCase(s2);
    }

    public static boolean startsWithIgnoreCase(String s1, String s2) {
        if (s1 == null || s2 == null) {
            return false;
        }
        return s1.toUpperCase().startsWith(s2.toUpperCase());
    }

    public static boolean isNotEmpty(String str) {
        return str != null && !str.isEmpty();
    }
}
