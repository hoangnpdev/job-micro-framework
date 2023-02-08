package nph.driver;

import org.apache.spark.sql.SparkSession;
import nph.utils.spark.ExtFunctions;

import java.util.*;

public class SparkBuilder {

    public static SparkSession getSparkByQueueName(String queueName, String appName) {
        SparkSession.Builder builder = builder(appName);
        SparkSession session = null;
        session = builder.master("yarn")
                .config("spark.yarn.queue", queueName)
                .config("spark.executor.instances", "80")
                .config("spark.driver.maxResultSize", "4g")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .getOrCreate();
        return session;
    }

    public static SparkSession getSparkByYarnConfig(YarnConfig yarnConfig, String appName, String queueName) {
        if (yarnConfig.equals(YarnConfig.LOCAL)) {
            return null;
        }
        SparkSession.Builder builder = getSparkBuilderByYarnConfig(yarnConfig, appName);
        if (Objects.nonNull(queueName)) {
            System.out.println("queue set: " + queueName);
            builder = builder.config("spark.yarn.queue", queueName);
        }
        SparkSession session = builder.getOrCreate();
        assert session != null;
        ExtFunctions.registerUdf(session);
        return session;

    }

    public static SparkSession getSparkByYarnConfig(YarnConfig yarnConfig, String appName) {
        return getSparkByYarnConfig(yarnConfig, appName, null);
    }

    private static SparkSession.Builder getSparkBuilderByYarnConfig(YarnConfig yarnConfig, String appName) {
        SparkSession.Builder builder = builder(appName);

        if (YarnConfig.DEV.equals(yarnConfig)) {
            builder = builder.master("yarn")
                    .config("spark.yarn.queue", "dev")
                    .config("spark.executor.instances", "150")
                    .config("spark.driver.maxResultSize", "6g")
                    .config("spark.driver.memory", "6g")
                    .config("spark.executor.memory", "5g");
        }

        if (YarnConfig.LLAP_MINI.equals(yarnConfig)) {
            builder = builder.master("yarn")
                    .config("spark.yarn.queue", "llap")
                    .config("spark.executor.instances", "3")
                    .config("spark.driver.maxResultSize", "5g")
                    .config("spark.driver.memory", "5g")
                    .config("spark.executor.memory", "5g");
        }

        if (YarnConfig.TRAINING.equals(yarnConfig)) {
            builder = builder.master("yarn")
                    .config("spark.yarn.queue", "training")
                    .config("spark.executor.instances", "100")
                    .config("spark.driver.maxResultSize", "6g")
                    .config("spark.driver.memory", "8g")
                    .config("spark.executor.memory", "4g");
        }

        if (YarnConfig.LLAP_DAILY.equals(yarnConfig)) {
            builder = builder.master("yarn")
                    .config("spark.yarn.queue", "llap")
                    .config("spark.executor.instances", "20")
                    .config("spark.driver.maxResultSize", "6g")
                    .config("spark.driver.memory", "6g")
                    .config("spark.executor.memory", "4g");
        }

        if (YarnConfig.LLAP_HIGHWAY.equals(yarnConfig)) {
            builder = builder.master("yarn")
                    .config("spark.yarn.queue", "llap")
                    .config("spark.executor.instances", "70")
                    .config("spark.driver.maxResultSize", "6g")
                    .config("spark.driver.memory", "6g")
                    .config("spark.executor.memory", "6g");
        }

        if (YarnConfig.LLAP_MAX_SPEED.equals(yarnConfig)) {
            builder = builder.master("yarn")
                    .config("spark.yarn.queue", "llap")
                    .config("spark.executor.instances", "120")
                    .config("spark.driver.maxResultSize", "8g")
                    .config("spark.driver.memory", "8g")
                    .config("spark.executor.memory", "6g");
        }

        if (YarnConfig.CUSTOM.equals(yarnConfig)) {
            builder = builder.master("yarn")
                    .config("spark.yarn.queue", "dev")
                    .config("spark.executor.instances", "50")
                    .config("spark.driver.maxResultSize", "6g")
                    .config("spark.driver.memory", "6g")
                    .config("spark.executor.memory", "4g");
        }

        if (YarnConfig.DEV_MEMORY.equals(yarnConfig)) {
            builder = builder.master("yarn")
                    .config("spark.yarn.queue", "dev")
                    .config("spark.executor.instances", "90")
                    .config("spark.driver.maxResultSize", "6g")
                    .config("spark.driver.memory", "8g")
                    .config("spark.executor.memory", "6g");
//                    .config("spark.sql.autoBroadcastJoinThreshold", "-1")

        }

        if (YarnConfig.DEV_MINI.equals(yarnConfig)) {
            builder = builder.master("yarn")
                    .config("spark.yarn.queue", "dev")
                    .config("spark.executor.instances", "20")
                    .config("spark.driver.maxResultSize", "5g")
                    .config("spark.driver.memory", "5g")
                    .config("spark.executor.memory", "5g");
        }
        if (YarnConfig.API.equals(yarnConfig)) {
            builder = builder.master("yarn")
                    .config("spark.yarn.queue", "api")
                    .config("spark.executor.instances", "70")
                    .config("spark.driver.maxResultSize", "5g")
                    .config("spark.driver.memory", "5g")
                    .config("spark.executor.memory", "5g");
        }
        return builder;
    }

    private static SparkSession.Builder builder(String appName) {
        return SparkSession.builder()
                .appName(appName)
                .enableHiveSupport()
                .config("spark.sql.files.ignoreMissingFiles", "true")
                .config("spark.sql.files.ignoreCorruptFiles", "true")
                .config("spark.ui.enabled", "true")
                .config("spark.ui.port", "10197")
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                .config("spark.executor.heartbeatInterval", "60")
                .config("spark.sql.autoBroadcastJoinThreshold", -1)
                .config("spark.sql.broadcastTimeout", 600)
                .config("spark.submit.deployMode", "client");// default: 64
    }


}
