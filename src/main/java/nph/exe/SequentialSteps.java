package nph.exe;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * SequentialSteps control sequential steps' execution
 */

public class SequentialSteps {

    private final Logger log = Logger.getLogger(SequentialSteps.class);

    private List<Step> stepList;

    private SequentialSteps(List<Step> stepList) {
        this.stepList = stepList;
    }

    public void run(Integer... userSkipStep) throws Exception {
        List<Integer> userSkipList = Arrays.asList(userSkipStep);
        for(int sid = 0; sid < stepList.size(); sid ++) {
            if (userSkipList.contains(sid)) {
                log.info("Step " + sid + " is skipped by user");
                continue;
            }
            Step step = stepList.get(sid);
            try {
                step.run();
            } catch (Exception e) {
                log.info("Step " + sid + " failed by " + ExceptionUtils.getStackTrace(e));
                if (!step.isSkippedOnFalse()) {
                    throw e;
                }
                log.info("Step " + sid + " is skipped on false");
            }
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final List<Step> stepList;

        public Builder() {
            stepList = new ArrayList<>();
        }

        public Builder addStep(Step step) {
            stepList.add(step);
            return this;
        }

        public SequentialSteps build() {
            return new SequentialSteps(this.stepList);
        }
    }
}
