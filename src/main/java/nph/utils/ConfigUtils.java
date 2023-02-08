package nph.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigUtils {

	public static Properties getProperties(String propertiesPath) throws IOException {
		InputStream file = new FileInputStream(propertiesPath);
		Properties props = new Properties();
		props.load(file);
		return props;
	}
}
