package nph.exe;

import org.apache.commons.cli.CommandLine;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import nph.aspect.JobConf;
import nph.driver.SparkBuilder;
import nph.utils.CommandLineUtils;
import nph.utils.PathUtils;
import nph.utils.spark.SparkContextPlus;

import java.util.Properties;

@Deprecated
public abstract class Job extends SparkContextPlus {

    public static Logger log = Logger.getLogger(Job.class);

    protected String jarDir;

    protected String jarPath;

    public static final String HOME_DATA = "/home/phunh/data/";

    public Properties properties;

    public void start(String[] args) throws Exception {
        jarPath = Job.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        jarDir = PathUtils.getParentPath(jarPath);
        config();
        report(args);
    }

    public abstract void config() throws Exception;

    public abstract void report(String[] args) throws Exception;

    public String getConfig(String key) {
        return properties.getProperty(key);
    }

    public void execute(String[] args) throws Exception {
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
        this.start(args);
        this.close();
    }

}
