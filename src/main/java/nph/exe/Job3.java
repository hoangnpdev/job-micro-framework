package nph.exe;

import org.apache.commons.cli.*;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import nph.aspect.JobConf;
import nph.driver.SparkBuilder;
import nph.utils.ConfigUtils;
import nph.utils.NotificationUtils;
import nph.utils.PathUtils;
import nph.utils.spark.SparkContextPlus;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Job3 class bind (params, configs, envs) to JobContext
 */
public abstract class Job3 extends SparkContextPlus {

    public static Logger log = Logger.getLogger(Job3.class);

    private static final String CONFIG_PATH = "config/application.properties";
    protected CommandLine cmd;

    protected String JOB_DIR;
    protected String JOB_PATH;

    @Param(value = "queue")
    protected String QUEUE;
    @Param(value = "job")
    protected String JOB;

    public void execute(String[] args) throws IOException {
        try {
            parseParams(args);
            bindParams();
            bindConfigs();
            bindEnvs();
            allocateResource();
            wireComponents();
            process();
            releaseResource();
        } catch (Exception e) {
            log.info(ExceptionUtils.getStackTrace(e));
            NotificationUtils.notifyError(this.getClass(), args, e);
        }
    }

    public void parseParams(String[] args) throws ParseException {
        List<Field> parameterizedFields = Arrays.stream(
                        FieldUtils.getAllFields(this.getClass())
                ).filter(f -> Objects.nonNull(f.getAnnotation(Param.class)))
                .collect(Collectors.toList());
        Options options = new Options();
        for (Field f : parameterizedFields) {
            String paramName = f.getAnnotation(Param.class).value();
            log.info("add param parsing: " + paramName);
            options.addOption(paramName, true, paramName);
        }
        CommandLineParser parser = new GnuParser();
        this.cmd = parser.parse(options, args);
    }

    public void bindParams() throws IllegalAccessException {
        List<Field> parameterizedFields = Arrays.stream(
                        FieldUtils.getAllFields(this.getClass())
                ).filter(f -> Objects.nonNull(f.getAnnotation(Param.class)))
                .collect(Collectors.toList());
        for (Field f : parameterizedFields) {
            String paramName = f.getAnnotation(Param.class).value();
            if (cmd.hasOption(paramName)) {
                String paramValue = cmd.getOptionValue(paramName);
                f.setAccessible(true);
                f.set(this, paramValue);
            }
        }
    }

    public void bindConfigs() throws IOException, IllegalAccessException {
        Properties props = ConfigUtils.getProperties(CONFIG_PATH);
        for (String pName : props.stringPropertyNames()) {
            log.info("key-value: " + pName + "-" + props.getProperty(pName));
        }
        List<Field> configuredFields = Arrays.stream(
                        FieldUtils.getAllFields(this.getClass())
                ).filter(f -> Objects.nonNull(f.getAnnotation(Config.class)))
                .collect(Collectors.toList());
        for (Field f : configuredFields) {
            String propertyName = f.getAnnotation(Config.class).value();
            String propertyValue = props.getProperty(propertyName);
            log.info("real key-value: " + propertyName + "-" + propertyValue);
            f.setAccessible(true);
            f.set(this, propertyValue);
        }
    }

    public void allocateResource() {
        JobConf jobConf = this.getClass().getDeclaredAnnotation(JobConf.class);
        // get&set queue is specific
        SparkSession spark;
        if (Objects.nonNull(QUEUE)) {
            System.out.println("queue name is overrode : " + cmd.getOptionValue("queue"));
            spark = SparkBuilder.getSparkByYarnConfig(jobConf.value(), this.getClass().getSimpleName(), cmd.getOptionValue("queue"));
        } else {
            spark = SparkBuilder.getSparkByYarnConfig(jobConf.value(), this.getClass().getSimpleName());
        }
        this.setSpark(spark);
    }

    public void bindEnvs() {
        JOB_PATH = Job3.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        JOB_DIR = PathUtils.getParentPath(JOB_PATH);
    }

    protected void wireComponents() {
        // wire job to components to use the components
    }

    public abstract void process() throws Exception;

    public void releaseResource() {
        this.close(); // release spark resource
    }

}
