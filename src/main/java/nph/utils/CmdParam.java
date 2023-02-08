package nph.utils;

@Deprecated
public enum CmdParam {
    QUEUE("queue"),
    JOB("job"),
    START_DATE("start_date"),
    END_DATE("end_date"),
    MODE("mode"),
    IS_VALIDATED("is_validated");


    public final String value;

    CmdParam(String value) {
        this.value = value;
    }
}
