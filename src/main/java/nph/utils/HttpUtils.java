package nph.utils;

import org.apache.log4j.Logger;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.stream.Collectors;

public class HttpUtils {

    private static final Logger log = Logger.getLogger(HttpUtils.class);

    public static String get(String uri) throws IOException {
        URL url = new URL(uri);
        HttpURLConnection httpURLConnection  = (HttpURLConnection) url.openConnection();
        httpURLConnection.setRequestMethod("GET");
        httpURLConnection.setRequestProperty("User-Agent", "Mozilla/5.0");


        int responseCode = httpURLConnection.getResponseCode();
        log.info(uri + " " + responseCode);
        if (responseCode == HttpURLConnection.HTTP_OK) {
            BufferedReader reader = new BufferedReader( new InputStreamReader(httpURLConnection.getInputStream()));
            return reader.lines().collect(Collectors.joining("\n"));
        }
        throw new RuntimeException("Request error: " + responseCode);
    }

    public static String postText(String uri, String body) throws IOException {
        URL url = new URL(uri);
        HttpURLConnection httpURLConnection  = (HttpURLConnection) url.openConnection();
        httpURLConnection.setRequestMethod("GET");
        httpURLConnection.setRequestProperty("User-Agent", "Mozilla/5.0");
        httpURLConnection.setRequestProperty("Content-Type", "plain/text");
        httpURLConnection.setDoOutput(true);

        writeBody(httpURLConnection.getOutputStream(), body);

        int responseCode = httpURLConnection.getResponseCode();
        log.info(uri + " " + responseCode);
        if (responseCode == HttpURLConnection.HTTP_OK) {
            BufferedReader reader = new BufferedReader( new InputStreamReader(httpURLConnection.getInputStream()));
            return reader.lines().collect(Collectors.joining("\n"));
        }
        throw new RuntimeException("Request error: " + responseCode);
    }

    public static String postJson(String uri, String body) throws IOException {
        URL url = new URL(uri);
        HttpURLConnection httpURLConnection  = (HttpURLConnection) url.openConnection();
        httpURLConnection.setRequestMethod("GET");
        httpURLConnection.setRequestProperty("User-Agent", "Mozilla/5.0");
        httpURLConnection.setRequestProperty("Content-Type", "application/json");
        httpURLConnection.setDoOutput(true);

        writeBody(httpURLConnection.getOutputStream(), body);

        int responseCode = httpURLConnection.getResponseCode();
        log.info(uri + " " + responseCode);
        if (responseCode == HttpURLConnection.HTTP_OK) {
            BufferedReader reader = new BufferedReader( new InputStreamReader(httpURLConnection.getInputStream()));
            return reader.lines().collect(Collectors.joining("\n"));
        }
        throw new RuntimeException("Request error: " + responseCode);
    }

    public static String post(String uri, String body) throws IOException {
        URL url = new URL(uri);
        HttpURLConnection httpURLConnection  = (HttpURLConnection) url.openConnection();
        httpURLConnection.setRequestMethod("GET");
        httpURLConnection.setRequestProperty("User-Agent", "Mozilla/5.0");
        httpURLConnection.setRequestProperty("Content-Type", "application/json");
        httpURLConnection.setDoOutput(true);

        writeBody(httpURLConnection.getOutputStream(), body);

        int responseCode = httpURLConnection.getResponseCode();
        log.info(uri + " " + responseCode);
        if (responseCode == HttpURLConnection.HTTP_OK) {
            BufferedReader reader = new BufferedReader( new InputStreamReader(httpURLConnection.getInputStream()));
            return reader.lines().collect(Collectors.joining("\n"));
        }
        throw new RuntimeException("Request error: " + responseCode);
    }


    private static void writeBody(OutputStream os, String body) throws IOException {
        OutputStreamWriter writer = new OutputStreamWriter(os);
        writer.write(body);
        writer.flush();
        writer.close();
        os.close();
    }
}
