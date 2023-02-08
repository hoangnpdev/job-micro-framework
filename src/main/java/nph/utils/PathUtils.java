package nph.utils;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import java.util.ArrayList;
import java.util.List;

public class PathUtils {

    public static String getParentPath(String path) {
        Iterable<String> e = Splitter.on("/").omitEmptyStrings().split(path);
        List<String> el = new ArrayList<>();
        e.forEach(el::add);
        List<String> sps = el.subList(0, el.size() - 1);
        return "/" + Joiner.on("/").join(sps) + "/";
    }
}
