package nph.exe;

public class Step {

    private boolean skippedOnFalse;

    private Executable execution;

    public Step(Executable execution, boolean skippedOnFalse) {
        this.execution = execution;
        this.skippedOnFalse = skippedOnFalse;
    }

    public static Step of(Executable execution) {
        return new Step(execution, false);
    }

    public static Step of(Executable execution, boolean skippedOnFalse) {
        return new Step(execution, skippedOnFalse);
    }

    public void run() throws Exception {
        execution.execute();
    }

    public boolean isSkippedOnFalse() {
        return skippedOnFalse;
    }

}
