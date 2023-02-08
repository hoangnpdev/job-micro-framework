package nph.utils;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class EncodeUtils {

    public static String encodeUTF8Base64(String input) {
        return Base64.getEncoder().encodeToString(input.getBytes(StandardCharsets.UTF_8));
    }

    public static String decodeUTF8Base64(String output) {
        return new String(Base64.getDecoder().decode(output));
    }
}
