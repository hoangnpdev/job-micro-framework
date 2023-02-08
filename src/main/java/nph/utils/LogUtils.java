package nph.utils;

import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class LogUtils {

    public static void setDefaultLogConfigs() {
        Properties props = new Properties();
        props.setProperty("log4j.rootLogger", "ERROR, stdout");
        props.setProperty("log4j.logger.nph", "INFO, stdout");
        props.setProperty("log4j.logger.org.apache.spark.scheduler.LiveListenerBus", "OFF");
        props.setProperty("log4j.additivity.nph", "false");

        props.setProperty("log4j.appender.stdout", "org.apache.log4j.ConsoleAppender");
        props.setProperty("log4j.appender.stdout.Target", "System.out");
        props.setProperty("log4j.appender.stdout.layout", "org.apache.log4j.PatternLayout");
        props.setProperty("log4j.appender.stdout.layout.ConversionPattern", "%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n");
        LogManager.resetConfiguration();
        PropertyConfigurator.configure(props);
    }

    public static void setConsoleFileLogConfigs(String dirName, String fileName) {
        File dir = new File(dirName);
        System.out.println(dirName + fileName);
        File file = new File(dirName + fileName);
        System.out.println("log - dir: " + dirName + fileName);
        try {
            if (!dir.exists()) {
                dir.mkdir();
                file.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("cannot create log file for: " + dirName + fileName);
        }

        Properties props = new Properties();
        props.setProperty("log4j.rootLogger", "ERROR, file, stdout");
        props.setProperty("log4j.logger.nph", "INFO, file, stdout");
        props.setProperty("log4j.logger.org.apache.spark.scheduler.LiveListenerBus", "OFF");
        props.setProperty("log4j.additivity.nph", "false");


        // file config
        props.setProperty("log4j.appender.file.layout", "org.apache.log4j.PatternLayout");
        props.setProperty("log4j.appender.file.layout.ConversionPattern", "%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n");
        props.setProperty("log4j.appender.file", "org.apache.log4j.RollingFileAppender");
        props.setProperty("log4j.appender.file.File", dirName + fileName);
        props.setProperty("log4j.appender.file.MaxFileSize", "10MB");
        props.setProperty("log4j.appender.file.MaxBackupIndex", "10");

        // stdout config
        props.setProperty("log4j.appender.stdout", "org.apache.log4j.ConsoleAppender");
        props.setProperty("log4j.appender.stdout.Target", "System.out");
        props.setProperty("log4j.appender.stdout.layout", "org.apache.log4j.PatternLayout");
        props.setProperty("log4j.appender.stdout.layout.ConversionPattern", "%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n");

        LogManager.resetConfiguration();
        PropertyConfigurator.configure(props);
    }
}
