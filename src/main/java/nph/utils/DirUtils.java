package nph.utils;

import org.apache.log4j.Logger;

import java.io.File;

public class DirUtils {

    private static Logger log = Logger.getLogger(DirUtils.class);

    public static void mkdirs(String path) {
        boolean isMkDirSuccess = new File(path).mkdirs();
        log.info("mkdirs " + path + ": " + isMkDirSuccess);
    }
}
