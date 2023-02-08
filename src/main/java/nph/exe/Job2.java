package nph.exe;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import nph.aspect.JobConf;
import nph.driver.SparkBuilder;
import nph.utils.CommandLineUtils;
import nph.utils.LogUtils;
import nph.utils.NotificationUtils;
import nph.utils.PathUtils;
import nph.utils.spark.SparkContextPlus;

import java.util.Properties;

@Deprecated
public abstract class Job2 extends SparkContextPlus {

    public static Logger log = Logger.getLogger(Job.class);

    protected String JAR_DIR;

    protected String JAR_PATH;

    public Properties properties;

    public void start(CommandLine cmd) throws Exception {
        JAR_PATH = Job2.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        JAR_DIR = PathUtils.getParentPath(JAR_PATH);
        config();
        report(cmd);
    }

    public void config() throws Exception {
        LogUtils.setDefaultLogConfigs();
    }

    ;

    public abstract void report(CommandLine cmd) throws Exception;

    public String getConfig(String key) {
        return properties.getProperty(key);
    }

    public void execute(String[] args) throws Exception {
        try {
            JobConf jobConf = this.getClass().getDeclaredAnnotation(JobConf.class);
            CommandLine cmd = CommandLineUtils.parseCommon(args);
            // get&set queue is specific
            SparkSession spark = null;
            if (cmd.hasOption("queue")) {
                System.out.println("queue name is overrode : " + cmd.getOptionValue("queue"));
                spark = SparkBuilder.getSparkByYarnConfig(jobConf.value(), this.getClass().getSimpleName(), cmd.getOptionValue("queue"));
            } else {
                spark = SparkBuilder.getSparkByYarnConfig(jobConf.value(), this.getClass().getSimpleName());
            }

            // set spark and start job
            this.setSpark(spark);
            this.start(cmd);
        } catch (Exception e) {
            log.info(ExceptionUtils.getStackTrace(e));
            NotificationUtils.notifyError(this.getClass(), args, e);
        }
        this.close();
    }

}