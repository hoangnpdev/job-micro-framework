package nph;

import org.apache.commons.cli.*;
import nph.exe.Job;
import nph.exe.Job2;
import nph.exe.Job3;
import nph.exe.QueryApp;
import nph.utils.LogUtils;

import java.lang.reflect.Constructor;

/**
 * JobLauncher run jobs with arguments
 */
public class JobLauncher {

    public static void main(String[] args) throws Exception {
        // set log
        LogUtils.setDefaultLogConfigs();

        // finding job
        String jobClassName = QueryApp.class.getCanonicalName();

        CommandLine cmd = parseCommon(args);
        if (cmd.hasOption("job")) {
            jobClassName = cmd.getOptionValue("job");
        }
        System.out.println(jobClassName);

        // get job and execute job
        Class jobClass = Class.forName(jobClassName);
        Constructor jobConstructor = jobClass.getConstructors()[0];
        Object job = jobConstructor.newInstance();
        if (job instanceof Job2) {
            ((Job2) job).execute(args);
        } else if (job instanceof Job) {
            ((Job) job).execute(args);
        } else {
            ((Job3) job).execute(args);
        }
    }

    public static CommandLine parseCommon(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("job", true, "Job full class name");

        CommandLineParser parser = new GnuParser();
        return parser.parse(options, args, true);
    }
}
