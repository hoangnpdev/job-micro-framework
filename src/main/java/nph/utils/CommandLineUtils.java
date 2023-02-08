package nph.utils;

import org.apache.commons.cli.*;

@Deprecated
public class CommandLineUtils {

    /**
     * -queue
     * @return queue_name
     */
//    public static CommandLine parseQueue(String[] args) throws ParseException {
//        Options options = new Options();
//        options.addOption("queue", true, "queue name's overrode for all job");
//        options.addOption("job", true, "job name - package + class name");
//
//        CommandLineParser parser = new BasicParser();
//        return parser.parse(options, args);
//    }

    public static CommandLine parseCommon(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption(CmdParam.QUEUE.value, true, "queue name's overrode for all job");
        options.addOption(CmdParam.JOB.value, true, "job name - package + class name");
        options.addOption(CmdParam.START_DATE.value, true, "start_date - yyyyMMdd");
        options.addOption(CmdParam.END_DATE.value, true, "end_date - yyyyMMdd");
        options.addOption(CmdParam.MODE.value, true, "mode");
        options.addOption(CmdParam.IS_VALIDATED.value, true, "is validated");

        CommandLineParser parser = new GnuParser();
        return parser.parse(options, args);
    }
}
