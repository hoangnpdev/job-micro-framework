package nph.utils;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class NotificationUtils {
	private static Logger log = Logger.getLogger(NotificationUtils.class);
	private static String dateLog;

	public static void notifyError(Class wh, String[] args, Exception e) throws IOException {
		Date date = new Date();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");
		String dateLog = format.format(date);
		TelegramBot.sendTelegram(
				"<b>" + dateLog + " " + wh.getCanonicalName() + " " + String.join(" ", args) + "</b>" +
						"\n" + ExceptionUtils.getMessage(e)
		);
		log.info(ExceptionUtils.getStackTrace(e));
	}

	public static void notifyError(Class wh, Exception e) throws IOException {
		Date date = new Date();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");
		String dateLog = format.format(date);
		TelegramBot.sendTelegram(
				"<b>" + dateLog + " " + wh.getCanonicalName() + "</b>" +
						"\n" + ExceptionUtils.getMessage(e)
		);
		log.info(ExceptionUtils.getStackTrace(e));
	}
}
