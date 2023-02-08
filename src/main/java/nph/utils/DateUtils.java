package nph.utils;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DateUtils {

    @SneakyThrows
    public static String reFormat(String date, String iFormat, String oFormat) {
        SimpleDateFormat i = new SimpleDateFormat(iFormat);
        SimpleDateFormat o = new SimpleDateFormat(oFormat);
        Date d = i.parse(date);
        return o.format(d);
    }

    public static Date str2Date(String value, String format) throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        return dateFormat.parse(value);
    }

    public static String date2Str(Date value, String format) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        return dateFormat.format(value);
    }

    public static long nDayBetweenDate(String start, String end) throws ParseException {
        return TimeUnit.DAYS.convert(
                DateUtils.str2Date(end, "yyyyMMdd").getTime() - DateUtils.str2Date(start, "yyyyMMdd").getTime(),
                TimeUnit.MILLISECONDS
        );
    }

    public static boolean isValidDate(String date, String format) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        try {
            dateFormat.parse(date);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    public static long nMonthBetweenDate(String start, String end) throws ParseException {
        Date startDate = DateUtils.str2Date(start, "yyyyMMdd");
        Date endDate = DateUtils.str2Date(end, "yyyyMMdd");
        return nMonthBetweenDate(startDate, endDate);
    }

    public static long nMonthBetweenDate(Date start, Date end) throws ParseException {
        Calendar startCalendar = Calendar.getInstance();
        startCalendar.setTime(start);

        Calendar endCalendar = Calendar.getInstance();
        endCalendar.setTime(end);
        return (endCalendar.get(Calendar.YEAR) - startCalendar.get(Calendar.YEAR)) * 12
                + (endCalendar.get(Calendar.MONTH) - startCalendar.get(Calendar.MONTH));
    }

    public static long nSecondsBetweenDate(String start, String end) throws ParseException {
        return TimeUnit.SECONDS.convert(
                DateUtils.str2Date(end, "yyyyMMdd").getTime() - DateUtils.str2Date(start, "yyyyMMdd").getTime(),
                TimeUnit.MILLISECONDS
        );
    }

    public static String nvl(Object object) {
        if (object == null) {
            return "null";
        }
        return object.toString();
    }

    public static Map<String, String> mapOfString(String input) {
        Map<String, String> result = new HashMap<>();
        Splitter.on(",")
                .trimResults()
                .split(input)
                .forEach(pair -> {
                    Iterator<String> es = Splitter.on(":")
                            .trimResults()
                            .split(pair).iterator();
                    List<String> o = Lists.newArrayList(es);
                    result.put(o.get(0), o.get(1));
                });
        return result;
    }

    public static boolean matchRegex(String input, String regex) {
        Pattern pattern = Pattern.compile(regex);
        return pattern.matcher(input).find();
    }

    public static String matchRegexAndReturnFirst(List<String> inputs, String regex) {
        for(String input: inputs) {
            if (matchRegex(input, regex)) {
                return input;
            }
        }
        return null;
    }

    public static List<String> extractRegex(String input, String regex) {
        List<String> result = Lists.newArrayList();
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(input);
        if (matcher.find()) {
            for (int i = 1; i <= matcher.groupCount(); i ++) {
                result.add(matcher.group(i));
            }
        }
        return result;
    }
    
    public static String getRegexAt(String input, String regex, int pos) {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(input);
        if (matcher.find()) {
            return matcher.group(pos);
        }
        return StringUtils.EMPTY;
    }

    public static String getFirstDateOfMonth(String input) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        Date date = str2Date(input, "yyyyMMdd");
        calendar.setTime(date);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        return date2Str(calendar.getTime(), "yyyyMMdd");
    }

    public static String getFirstDateOfLastMonth(String input) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        Date date = str2Date(input, "yyyyMMdd");
        calendar.setTime(date);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.add(Calendar.MONTH, -1);
        return date2Str(calendar.getTime(), "yyyyMMdd");
    }

    public static String getFirstDateOfLastMonth() throws ParseException {
        Calendar calendar = Calendar.getInstance();
        Date date = new Date();
        calendar.setTime(date);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.add(Calendar.MONTH, -1);
        return date2Str(calendar.getTime(), "yyyyMMdd");
    }

    public static String getFirstDateOfMonth() throws ParseException {
        Calendar calendar = Calendar.getInstance();
        Date date = new Date();
        calendar.setTime(date);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        return date2Str(calendar.getTime(), "yyyyMMdd");
    }

    public static String getLastDateOfMonth(String input) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        Date date = str2Date(input, "yyyyMMdd");
        calendar.setTime(date);
        calendar.add(Calendar.MONTH, 1);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.add(Calendar.DATE, -1);
        return date2Str(calendar.getTime(), "yyyyMMdd");
    }

    public static String getLastDateOfLastMonth(String input) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        Date date = str2Date(input, "yyyyMMdd");
        calendar.setTime(date);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.add(Calendar.DATE, -1);
        return date2Str(calendar.getTime(), "yyyyMMdd");
    }

    public static String getLastDateOfLastMonth() throws ParseException {
        Calendar calendar = Calendar.getInstance();
        Date date = new Date();
        calendar.setTime(date);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.add(Calendar.DATE, -1);
        return date2Str(calendar.getTime(), "yyyyMMdd");
    }

    public static String getLastDateOfXxxMonthBefore(String input, int Xxx) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        Date date = str2Date(input, "yyyyMMdd");
        calendar.setTime(date);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.add(Calendar.MONTH, -Xxx + 1);
        calendar.add(Calendar.DATE, -1);
        return date2Str(calendar.getTime(), "yyyyMMdd");
    }

    public static String getLastDateOfXxxMonthBefore(int Xxx) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        Date date = new Date();
        calendar.setTime(date);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.add(Calendar.MONTH, -Xxx + 1);
        calendar.add(Calendar.DATE, -1);
        return date2Str(calendar.getTime(), "yyyyMMdd");
    }

    public static String getToday() {
        return date2Str(new Date(), "yyyyMMdd");
    }

    public static String getLastDay() {
        Calendar calendar = Calendar.getInstance();
        Date date = new Date();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        return date2Str(calendar.getTime(), "yyyyMMdd");
    }

    public static String oneDayBefore(String strDate) throws ParseException {
        Date date = str2Date(strDate, "yyyyMMdd");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DATE, -1);
        return date2Str(calendar.getTime(), "yyyyMMdd");
    }

    public static String nextDay(String strDate, int nDay) throws ParseException {
        Date date = str2Date(strDate, "yyyyMMdd");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DATE, nDay);
        return date2Str(calendar.getTime(), "yyyyMMdd");
    }

    /**
     * bug right column name. But wrong ID (unknown cause)
     * use toArray to fix this
     * @param old
     * @param news
     * @return
     */
    public static Seq<Column> toSeq(String[] old, String... news) {
        String[] result = Arrays.copyOf(old, old.length + news.length);
        System.arraycopy(news, 0, result, old.length, news.length);
        return JavaConverters.asScalaIteratorConverter(
                Arrays.stream(result).map(functions::col).collect(Collectors.toList()).iterator()
        ).asScala().toSeq();
    }

    /**
     *
     * @param old
     * @param news
     * @return
     */
    public static Column[] toArr(String[] old, String... news) {
        Seq<Column> cSeq = toSeq(old, news);
        Column[] cols = new ArrayList<Column>(
                JavaConversions.seqAsJavaList(cSeq)
        ).toArray(new Column[cSeq.size()]);
        return cols;
    }

    public static String monthOf(String yyyyMMdd) throws ParseException {
        Date date = str2Date(yyyyMMdd, "yyyyMMdd");
        return date2Str(date, "MM");
    }

    public static String yearOf(String yyyyMMdd) throws ParseException {
        Date date = str2Date(yyyyMMdd, "yyyyMMdd");
        return date2Str(date, "yyyy");
    }

    public static String reFormatDate(String yyyyMMdd, String newFormat) throws ParseException {
        Date date = str2Date(yyyyMMdd, "yyyyMMdd");
        return date2Str(date, newFormat);
    }

    public static String reFormatDate(String dateStr, String oldFormat, String newFormat) throws ParseException {
        Date date = str2Date(dateStr, oldFormat);
        return date2Str(date, newFormat);
    }

    public static String[] listBetweenDates(String startDate, String endDate, String outputFormat) throws ParseException {
        ArrayList<String> dates = new ArrayList<>();
        Date sd = DateUtils.str2Date(startDate, "yyyyMMdd");
        Date ed = DateUtils.str2Date(endDate, "yyyyMMdd");
        Date cd = DateUtils.str2Date(startDate, "yyyyMMdd");
        Calendar calendar = Calendar.getInstance();
        while (!cd.after(ed)) {

            dates.add(DateUtils.date2Str(cd, outputFormat));

            calendar.setTime(cd);
            calendar.add(Calendar.DAY_OF_MONTH, 1);
            cd = calendar.getTime();
        }
        return dates.toArray(new String[dates.size()]);
    }

    public static boolean isFirstDayOfMonth(String date) {
        return date.substring(6).equals("01");
    }

    public static boolean isWeekend(String dateStr) throws ParseException {
        Date date = DateUtils.str2Date(dateStr, "yyyyMMdd");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return Calendar.SUNDAY == calendar.get(Calendar.DAY_OF_WEEK);
    }

}
