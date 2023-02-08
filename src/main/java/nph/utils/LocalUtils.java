package nph.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Deprecated
public class LocalUtils {

    private static final Logger log = Logger.getLogger(LocalUtils.class);

    public static Pair<List<Row>, StructType> loadCsv(String localFile) throws IOException {
        List<Row> result = new ArrayList<>();
        BufferedReader file = Files.newBufferedReader(Paths.get(localFile), StandardCharsets.UTF_8);
        String line = file.readLine();
        int size = line.split(",").length;
        while (line != null) {
            String[] cells = line.split(",");
            result.add(RowFactory.create(cells));
            line = file.readLine();
        }

        List<StructField> fields = new ArrayList<>();
        for (int i = 0; i < size; i ++) {
            fields.add(DataTypes.createStructField("_c" + i, DataTypes.StringType, false));
        }
        StructType schema = DataTypes.createStructType(fields);
        file.close();
        return Pair.of(result, schema);
    }

    public static List<String> loadFileAsString(String localFile) throws IOException {
        List<String> result = new ArrayList<>();
        BufferedReader file = Files.newBufferedReader(Paths.get(localFile), StandardCharsets.UTF_8);
        String line = file.readLine();
        while (line != null) {
            result.add(line);
            line = file.readLine();
        }
        return result;
    }

    public static List<String> loadFileAsString(String localFile, Long start, Long end) throws IOException {

        List<String> result = new ArrayList<>();
        BufferedReader file = Files.newBufferedReader(Paths.get(localFile), StandardCharsets.UTF_8);
        long index = 0;
        String line = file.readLine();
        while (line != null && index < end) {
            line = file.readLine();
            if (index < start) {
                index ++;
                continue;
            }
            result.add(line);
            index ++;

        }
        return result;
    }

    public static void saveAsJson(Object object, String filePath) {
        try {

            ObjectMapper objectMapper = new ObjectMapper();
            Writer fileWriter = new FileWriter(filePath, false);
            String output = objectMapper.writeValueAsString(object);
            System.out.println("output: " + output);
            fileWriter.write(output);
            fileWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void saveAsCsv(List<String> rows, String filePath) {
        try {
            new File(filePath).createNewFile();
            FileWriter fileWriter = new FileWriter(filePath, false);
            for (String row: rows) {
                fileWriter.write(row + "\n");
            }
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void saveAsCsv(List<String> rows, String dirName, String fileName) {
        log.info("save as: " + dirName + fileName);
        File dir = new File(dirName);
        dir.mkdirs();
        File file = new File(dirName + fileName);
        try {
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Create new file not succeed");
        }

        saveAsCsv(rows, dirName + fileName);

    }

    public static void mkdirs(String path) {
        File f = new File(path);
        log.info("mkdir status: " + f.mkdirs() + "- " + path);
    }

}
