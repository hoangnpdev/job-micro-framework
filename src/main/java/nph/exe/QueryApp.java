package nph.exe;

import nph.aspect.JobConf;
import nph.driver.YarnConfig;

@JobConf(YarnConfig.LOCAL)
public class QueryApp extends Job3 {



    @Param("test_param")
    private String TEST_PARAM;

    @Config("nph.job.test")
    private String TEST_CONFIG;



    @Override
    protected void wireComponents() {
    }

    @Override
    public void process() throws Exception {
        log.info(QUEUE);
        log.info("test param: " + TEST_PARAM);
        log.info("test config: " + TEST_CONFIG);
//        log.info("count data: " + kmcbRepository.getKmcbLastCreatedFrame("20230131").count());
    }
}
