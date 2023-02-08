package nph.utils;

import java.util.Objects;

public class StrUtils {

    public static String removeDuplicateSpace(String value) {
        if (Objects.nonNull(value)) {
            return value.replaceAll("\\s+", " ");
        }
        return "";
    }
    public static String removeSpace(String value) {
        if (Objects.nonNull(value)) {
            return value.replaceAll("\\s+", "");
        }
        return "";
    }
}
