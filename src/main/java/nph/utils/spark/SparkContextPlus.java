package nph.utils.spark;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import nph.utils.DateUtils;
import nph.utils.LocalUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.lit;

/**
 * SparkContextPlus provide spark utility
 */
public class SparkContextPlus {

    protected SparkSession spark;

    protected FileSystem fs;

    protected static final String MILESTONE_TEMP_DIR = "hdfs:///user/phunh/temp/";

    private int milestone = 0;

    private boolean milestoneFlag = true;

    private boolean milestoneReset = false;

    private boolean sparkSessionClosed;

    public SparkContextPlus() {

    }

    public void setSpark(SparkSession spark) {
        this.spark = spark;
        this.sparkSessionClosed = false;
        try {
            if (spark != null) {
                fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
            }
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }

    }

    public SparkSession getSpark() {
        return this.spark;
    }

    public void close() {
        if (!this.sparkSessionClosed && this.spark != null) {
            spark.close();
        }
        this.sparkSessionClosed = true;
    }

    public Dataset<Row> sql(String query, Object... args) {
        String builtQuery = String.format(query, args);
        return spark.sql(builtQuery);
    }

    public Dataset<Row> loadBigCsv(String localPath) throws IOException {
        return loadBigCsv(localPath, false);
    }

    public Dataset<Row> loadBigCsv(String localPath, boolean header) throws IOException {
        return loadBigCsv(localPath, header, ",");
    }

    public Dataset<Row> loadBigCsv(String localPath, boolean header, String sep) throws IOException {
        String uuid = UUID.randomUUID().toString();
        String dir = "hdfs:///user/phunh/load/";
        String path =  dir + "load-" + uuid;
        if (fs.exists(new Path(path))) {
            fs.delete(new Path(path), true);
        }
        fs.mkdirs(new Path(dir));
        fs.copyFromLocalFile(new Path(localPath), new Path(path));
        return spark.read().format("csv")
                .option("header", String.valueOf(header))
                .option("sep", sep)
                .load(path);
    }

    public Dataset<Row> loadBigCsv(String sep, String... localPaths) throws IOException {
        ArrayList<String> pathList = new ArrayList<>();

        for (String localPath: localPaths) {
            String uuid = UUID.randomUUID().toString();
            String dir = "hdfs:///user/phunh/load/";
            String path = dir + "load-" + uuid;
            if (fs.exists(new Path(path))) {
                fs.delete(new Path(path), true);
            }
            fs.mkdirs(new Path(dir));
            fs.copyFromLocalFile(new Path(localPath), new Path(path));
            pathList.add(path);
        }
        return spark.read().format("csv")
                .option("header", "false")
                .option("sep", sep)
                .load(pathList.toArray(new String[pathList.size()]));
    }

    public void saveBigCsv(Dataset<Row> frame, String localPath) throws IOException {
        saveBigCsv(frame, localPath, false);
    }

    public void saveBigCsv(Dataset<Row> frame, String localPath, boolean header) throws IOException {
        saveBigCsv(frame, localPath, header, ",");
    }

    public void saveBigCsv(Dataset<Row> frame, String localPath, boolean header, String sep) throws IOException {

        String hdfsDir = "hdfs:///user/phunh/save/" + UUID.randomUUID().toString();
        System.out.println("hdfsDir: " + hdfsDir);
        frame.write().option("sep", sep).option("header", "false").mode(SaveMode.Overwrite).csv(hdfsDir);
        RemoteIterator<LocatedFileStatus> remoteFiles = fs.listFiles(new Path(hdfsDir), false);
        BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(localPath, false));

        if (header) {
            String headers = String.join(sep, frame.columns());
            headers = headers + "\n";
            outputStream.write(headers.getBytes());
        }

        while(remoteFiles.hasNext()) {
            LocatedFileStatus rFile = remoteFiles.next();
            if (rFile.isFile()) {
                InputStream inputStream = fs.open(rFile.getPath());
                IOUtils.copyBytes(inputStream, outputStream, 5242880);
                inputStream.close();
            }
        }
        outputStream.close();
    }

    public void saveCsvOnHdfs(Dataset<Row> frame, String hdfsPath, String fName) throws IOException {
        fs.mkdirs(new Path(hdfsPath + "tmp"));
        frame.repartition(1).write()
                .option("header", "false")
                .mode(SaveMode.Overwrite)
                .csv(hdfsPath + "tmp");


        FileStatus[] fileStatuses = fs.listStatus(new Path(hdfsPath + "tmp"));
        List<String> fileNames = Arrays.stream(fileStatuses).sequential()
                .map(fileStatus -> fileStatus.getPath().getName()).collect(Collectors.toList());

        String fileName = DateUtils.matchRegexAndReturnFirst(fileNames, "^part-.*$");

        Path sourcePath = new Path(hdfsPath + "tmp" + "/" + fileName);
        Path destinationPath = new Path(hdfsPath + fName);
        fs.delete(destinationPath, true);
        fs.rename(sourcePath, destinationPath);
        fs.delete(new Path(hdfsPath + "tmp"), true);
    }

    public Stream<String> loadBigCsvAsStream(String hdfsPath) throws IOException {
        RemoteIterator<LocatedFileStatus> remoteFiles = fs.listFiles(new Path(hdfsPath), false);
        List<InputStream> inputStreamList = new ArrayList<>();
        while(remoteFiles.hasNext()) {
            LocatedFileStatus rFile = remoteFiles.next();
            if (rFile.isFile()) {
                InputStream inputStream = fs.open(rFile.getPath());
                inputStreamList.add(inputStream);
            }
        }
        Enumeration<InputStream> inputStreamEnumeration = Collections.enumeration(inputStreamList);
        InputStream result = new SequenceInputStream(inputStreamEnumeration);
        BufferedReader pre = new BufferedReader(new InputStreamReader(result, StandardCharsets.UTF_8));
        return pre.lines();
    }

    public Stream<String> streamFrame(Dataset<Row> frame) throws IOException {
        String hdfsDir = "hdfs:///user/phunh/save/" + UUID.randomUUID().toString();
        frame.write().option("header", "false").mode(SaveMode.Overwrite).csv(hdfsDir);
        RemoteIterator<LocatedFileStatus> remoteFiles = fs.listFiles(new Path(hdfsDir), false);
        List<InputStream> inputStreamList = new ArrayList<>();
//        BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(localPath, false));
        while(remoteFiles.hasNext()) {
            LocatedFileStatus rFile = remoteFiles.next();
            if (rFile.isFile()) {
                InputStream inputStream = fs.open(rFile.getPath());
                inputStreamList.add(inputStream);
            }
        }
        SequenceInputStream sequenceInputStream = new SequenceInputStream(Collections.enumeration(inputStreamList));
        return new BufferedReader(new InputStreamReader(sequenceInputStream)).lines();
    }

    public Dataset<Row> loadCsv(String csvPath) throws IOException {
        Pair<List<Row>, StructType> memberMeta = LocalUtils.loadCsv(csvPath);
        return spark.createDataFrame(memberMeta.getKey(), memberMeta.getValue());
    }

    public void setMilestoneFlag(boolean milestoneFlag) {
        this.milestoneFlag = milestoneFlag;
    }

    public void setMilestoneReset(boolean milestoneReset) {
        this.milestoneReset = milestoneReset;
    }

    /**
     * cache dataset on hdfs disk. Datasetset should small < 100tr row. ratio 1:5, 1:10
     * @param dataset
     * @param tempName
     * @return
     * @throws IOException
     */
    public Dataset<Row> mileStone(Dataset<Row> dataset, String tempName) throws IOException {
        if (!milestoneFlag) {
            return dataset;
        }

        String pathName = MILESTONE_TEMP_DIR + tempName;
        if (!fs.exists(new Path(pathName)) && !milestoneReset) {
            // if not exist and milestonReset = false: caculate this dataset  and persist this dataset as csv
            dataset.write().option("header", "true").csv(pathName);

        }
        return spark.read().format("csv").option("header", "true").load(pathName);
    }

    @Deprecated
    public Dataset<Row> mileStone(Dataset<Row> dataset) throws IOException {
        String tempName = "temp-" + milestone;
        milestone ++;
        return mileStone(dataset, tempName);
    }

    public Dataset<Row> subtractByColumn(Dataset<Row> first, Dataset<Row> second, String columnName) {
        String columnName0 = columnName + "0";
        Dataset<Row> second0 = second.withColumnRenamed(columnName, columnName0);
        return first.join(
                second0,
                first.col(columnName).equalTo(second0.col(columnName0)),
                "leftanti"
        ).select(DateUtils.toArr(first.columns()));
    }

    public Dataset<Row> subtractByColumn(Dataset<Row> first, Dataset<Row> second, String... columnNameList) {
        for (String columnName: columnNameList) {
            second = second.withColumnRenamed(columnName, columnName + "0");
        }
        Column joinCondition = lit(true).equalTo(lit(true));
        for (String columnName: columnNameList) {
            joinCondition = joinCondition.and(first.col(columnName).equalTo(second.col(columnName + "0")));
        }
        return first.join(
                second,
                joinCondition,
                "leftanti"
        ).select(DateUtils.toArr(first.columns()));
    }

    public Dataset<Row> createEmptyFrame(String[] fields) {
        Dataset<Row> emptyFrame = spark.emptyDataFrame();
        for (String field: fields) {
            emptyFrame = emptyFrame.withColumn(field, lit(""));
        }
        return emptyFrame;
    }

    public void saveAsHive(Dataset<Row> dataset, String tableName) {
        spark.sql("set hive.exec.dynamic.partition=true");
        spark.sql("set hive.exec.dynamic.partition.mode=nonstrict");
        Dataset<Row> table = spark.sql("select * from " + tableName);
        dataset = dataset.select(DateUtils.toSeq(table.columns()));
        dataset.write().mode(SaveMode.Overwrite).insertInto(tableName);
    }
}
