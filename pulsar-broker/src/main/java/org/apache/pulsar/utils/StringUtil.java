package org.apache.pulsar.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtil {
    private static String regEx = "[ _`~!@#$%^&*()+=|{}':;',\\[\\].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？]|\n|\r|\t";
    private static Pattern p = Pattern.compile(regEx);

    public static boolean isSpecialChar(String str) {
        Matcher m = p.matcher(str);
        return m.find();
    }
}
