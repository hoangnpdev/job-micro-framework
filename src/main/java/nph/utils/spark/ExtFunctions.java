package nph.utils.spark;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;
import nph.utils.DateUtils;

import java.text.ParseException;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class ExtFunctions {

    public static Column formatTime(Column col, String from, String to) {
        return functions.callUDF("format_time", col, lit(from), lit(to));
    }

    public static Column formatTimeMultipleCase(Column col, String from1, String from2, String to) {
        return functions.callUDF("format_multiple_time", col, lit(from1), lit(from2), lit(to));
    }

    public static Column getJson(Column col, String key) {
        return functions.callUDF("get_json_value", col, lit(key));
    }

    public static Column regexAt(Column col, String regex, Integer grPos) {
        return functions.callUDF("regex_at", col, lit(regex), lit(grPos));
    }

    public static Column ifElse(Column condition, Column ifCol, Column elseCol) {
        return when(condition, ifCol).otherwise(elseCol);
    }

    public static Column countDistinctToken(Column col) {
        return callUDF("count_distinct_token", col);
    }

    public static Column getMostToken(Column col) {
        return callUDF("get_most_token", col);
    }

    public static Column getTokenAt(Column col, String separator, Integer pos) {
        return callUDF("get_token_at", col, lit(separator), lit(pos));
    }

    public static Column isDigit(Column col) {
        return callUDF("is_digit", col);
    }

    public static Column addZeroPrefix(Column col, Integer fixedLength) {
        return callUDF("add_zero_prefix", col, lit(fixedLength));
    }

    public static Column map2json(Column column) {
        return callUDF("map_2_json", column);
    }

    public static Column listMap2Map(Column column) {
        return callUDF("list_map_2_map", column);
    }


    public static void registerUdf(SparkSession spark) {

        spark.udf().register(
                "add_zero_prefix",
                (String value, Integer fixedLength) -> {
                    Integer valueLength = value.length();
                    int nZero = fixedLength - valueLength;
                    StringBuilder valueBuilder = new StringBuilder(value);
                    for (int i = 0; i < nZero; i ++) {
                        valueBuilder.insert(0, "0");
                    }
                    value = valueBuilder.toString();
                    return value;
                }, DataTypes.StringType
        );

        spark.udf().register(
                "is_digit",
                (String value) -> StringUtils.isNumeric(value),
                DataTypes.BooleanType
        );

        spark.udf().register(
                "get_token_at",
                (String line, String sep, Integer pos) -> {
                    if (line != null) {
                        String[] tokens = line.split(sep);
                        if (pos < tokens.length)
                            return tokens[pos];
                    }
                    return "";
                }, DataTypes.StringType
        );

        spark.udf().register(
                "count_distinct_token",
                (String tokenStr)
                        -> Arrays.stream(tokenStr.split(","))
                        .filter(StringUtils::isNotEmpty)
                        .distinct()
                        .count()
                , DataTypes.LongType
        );

        spark.udf().register(
                "get_most_token",
                (String tokenStr) -> {
                    Optional<Map.Entry<String, Long>> mostToken = Arrays.stream(tokenStr.split(","))
                            .filter(StringUtils::isNotEmpty)
                            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                            .entrySet()
                            .stream()
                            .max(Map.Entry.comparingByValue());
                    return mostToken.isPresent() ? mostToken.get().getKey() : StringUtils.EMPTY;
                }
                , DataTypes.StringType
        );



        spark.udf().register(
                "regex_at",
                (String text, String regex, Integer grPos) -> {
                    Pattern pattern = Pattern.compile(regex);
                    Matcher matcher = pattern.matcher(text);
                    if (matcher.find()) {
                        return matcher.group(grPos);
                    }
                    return "";
                }, DataTypes.StringType
        );

        spark.udf().register(
                "get_json_value",
                (String json, String key) -> {
                    if (StringUtils.isEmpty(json)) {
                        return "";
                    }
                    ObjectMapper jsonMapper = new ObjectMapper();
                    JsonNode root = jsonMapper.readTree(json);
                    JsonNode value = root.get(key);
                    return value == null ? "" : value.getTextValue();
                },
                DataTypes.StringType
        );

        spark.udf().register(
                "format_time",
                (String date, String from, String to) -> {
                    Date tempDate = DateUtils.str2Date(date, from);
                    return DateUtils.date2Str(tempDate, to);
                },
                DataTypes.StringType
        );

        spark.udf().register(
                "format_multiple_time",
                (String date, String from, String from1, String to) -> {
                    try {
                        Date tempDate = DateUtils.str2Date(date, from);
                        return DateUtils.date2Str(tempDate, to);
                    } catch (ParseException e) {
                        try {
                            Date tempDate = DateUtils.str2Date(date, from1);
                            return DateUtils.date2Str(tempDate, to);
                        } catch (ParseException e1) {
                            return "";
                        }
                    }
                },
                DataTypes.StringType
        );

        spark.udf().register(
                "is_valid_date",
                (Integer date, String format) -> DateUtils.isValidDate(String.valueOf(date), format), DataTypes.BooleanType
        );

        spark.udf().register(
                "date2simAge",
                (String date, String curDate) -> {
                    if (date == null) {
                        return "-1";
                    }
                    try {
                        String fPeriod;
                        long period = DateUtils.nMonthBetweenDate(date, curDate);
                        if (period < 3) {
                            period = DateUtils.nDayBetweenDate(date, curDate);
                            fPeriod = period + "N";
                        } else {
                            fPeriod = period + "T";
                        }
                        return fPeriod;
                    } catch (ParseException e) {
                        return "-1";
                    }
                }, DataTypes.StringType
        );

        // birth to age
        spark.udf().register(
                "birth2age",
                (String birthDay) -> {
                    if (birthDay == null) {
                        return -1L;
                    }
                    try {
                        Date date = DateUtils.str2Date(birthDay, "yyyyMMdd");
                        String year = DateUtils.date2Str(date, "yyyy");
                        Long yearL = Long.parseLong(year);
                        Long curYearL = Long.parseLong(DateUtils.date2Str(new Date(), "yyyy"));
                        return curYearL - yearL;
                    } catch (ParseException e) {
                        return -1L;
                    }
                }, DataTypes.LongType
        );

        spark.udf().register(
                "list2string",
                (WrappedArray<String> arr) -> {
                    List<String> prefixs = JavaConversions.seqAsJavaList(arr);
                    return String.join(",", prefixs);
                }, DataTypes.StringType
        );

        spark.udf().register(
                "map_2_json",
                (scala.collection.immutable.Map<String, String> map) -> {
                    ObjectMapper jsonMapper = new ObjectMapper();
                    return jsonMapper.writeValueAsString(JavaConversions.mapAsJavaMap(map));
                }, DataTypes.StringType
        );

        spark.udf().register(
                "list_map_2_map",
                (WrappedArray<scala.collection.immutable.Map<String, String>> listEntry) -> {
                    List<scala.collection.immutable.Map<String, String>> listE = JavaConversions.seqAsJavaList(listEntry);
                    Map<String, String> result = new HashMap<>();
                    for (scala.collection.immutable.Map<String, String> e: listE) {
                        Map<String, String> je = JavaConversions.mapAsJavaMap(e);
                        result.putAll(je);
                    }
                    return JavaConversions.mapAsScalaMap(result);
                }, DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)
        );

        // list_of_map 2 map
        spark.udf().register(
                "list_entry_2_map",
                (WrappedArray<scala.collection.immutable.Map<String, Long>> listEntry) -> {
                    List<scala.collection.immutable.Map<String, Long>> listE = JavaConversions.seqAsJavaList(listEntry);
                    Map<String, Long> result = new HashMap<>();
                    for (scala.collection.immutable.Map<String, Long> e: listE) {
                        Map<String, Long> je = JavaConversions.mapAsJavaMap(e);
                        result.putAll(je);
                    }
                    return JavaConversions.mapAsScalaMap(result);
                }, DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType)
        );

        // is84Normal
        spark.udf().register(
                "isPhoneLenValid",
                (String phone) -> DateUtils.matchRegex(phone, "^[0-9]{9,11}$"),
                DataTypes.BooleanType
        );

        // convert string date to long month
        spark.udf().register(
                "strDate2Mon",
                (String date) -> {
                    long month = Long.parseLong(date);
                    return month / 100L;
                },
                DataTypes.LongType
        );

        spark.udf().register(
                "date2Mon",
                (Integer date) -> date / 100L,
                DataTypes.LongType
        );

        //
        spark.udf().register(
                "toPrefixNumber",
                (String pn) -> {
                    List<String> prefix_set = Arrays.asList("8488", "8491", "8494", "8481", "8482", "8483", "8484", "8485");
                    String prefix = pn.substring(0, 4);
                    if (!prefix_set.contains(prefix)) {
                        return "other";
                    }
                    return prefix;
                },
                DataTypes.StringType
        );

        //
        spark.udf().register(
                "time2hour",
                (String time) -> {
                    Date date = DateUtils.str2Date(time, "yyyyMMddHHmmss");
                    return DateUtils.date2Str(date, "HH");
                },
                DataTypes.StringType
        );

        //
        spark.udf().register(
                "time2day",
                (String time) -> {
                    Date date = DateUtils.str2Date(time, "yyyyMMddHHmmss");
                    return DateUtils.date2Str(date, "dd");
                },
                DataTypes.StringType
        );


    }
}
