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

    public static boolean endsWithIgnoreCase(String str, String suffix) {
        return endsWith(str, suffix, true);
    }

    private static boolean endsWith(String str, String suffix, boolean ignoreCase) {
        if (str != null && suffix != null) {
            if (suffix.length() > str.length()) {
                return false;
            } else {
                int strOffset = str.length() - suffix.length();
                return str.regionMatches(ignoreCase, strOffset, suffix, 0, suffix.length());
            }
        } else {
            return str == null && suffix == null;
        }
    }

    public static boolean containsIgnoreCase(String str, String searchStr) {
        if (str != null && searchStr != null) {
            int len = searchStr.length();
            int max = str.length() - len;

            for(int i = 0; i <= max; ++i) {
                if (str.regionMatches(true, i, searchStr, 0, len)) {
                    return true;
                }
            }

            return false;
        } else {
            return false;
        }
    }
}
