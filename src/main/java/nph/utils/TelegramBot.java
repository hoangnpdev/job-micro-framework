package nph.utils;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

public class TelegramBot {
	// get https://api.telegram.org/bot{bot_token}/sendMessage
	//5027499444:AAH7JJBUX2pl7mdCKG5UlT23baafatO7Ltg

//	private static HttpClient buildWebClient() {
//		HttpClient client = HttpClientBuilder.getInstance()
//				.addRequestFilter(new UserRequestFilter())
//				.addResponseFilter(new UserResponseFilter())
//				.addConnectionOptions(new ConnectionOption<>("Timeout", 2000))
//				.build();
//	}

//	public static String post(String uri, String jsonBody) {
//		WebClient.ResponseSpec responseSpec = webClient.post().uri(uri)
//				.contentType(MediaType.APPLICATION_JSON)
//				.body(BodyInserters.fromValue(jsonBody))
//				.retrieve();
//		return responseSpec.bodyToMono(String.class).block();
//	}

	public static String get(String uri) throws IOException {
		URL url = new URL(uri);
		HttpURLConnection httpURLConnection  = (HttpURLConnection) url.openConnection();
		httpURLConnection.setRequestMethod("GET");
		httpURLConnection.setRequestProperty("User-Agent", "Mozilla/5.0");
		int responseCode = httpURLConnection.getResponseCode();
		System.out.println(uri);
		System.out.println(responseCode);
		System.out.println(httpURLConnection.getResponseMessage());
		if (responseCode == HttpURLConnection.HTTP_OK) {
			BufferedReader reader = new BufferedReader( new InputStreamReader(httpURLConnection.getInputStream()));
			return reader.lines().collect(Collectors.joining("\n"));
		}
		return "request error";
	}

	public static String sendTelegram(String message) throws IOException {
		String url = "";
		String uri = "" +
				"&parse_mode=HTML" +
				"&text=" + URLEncoder.encode(message, StandardCharsets.UTF_8.toString());
		String res = get(url + uri);
		System.out.println(res);
		return res;
	}


}
