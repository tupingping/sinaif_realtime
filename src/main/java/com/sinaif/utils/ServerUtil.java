package com.sinaif.utils;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;

public class ServerUtil {

	public static String printStackTrace(Throwable e) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw, true);
		e.printStackTrace(pw);
		if (pw != null) {
			pw.close();
		}
		if (sw != null) {
			try {
				sw.close();
			} catch (IOException e1) {
				// e1.printStackTrace();
			}
		}
		return sw.toString();
	}

	public static Properties getConfigProperties() {
		Properties properties = new Properties();
		try {
			properties.load(ClassLoader.getSystemResourceAsStream("config.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return properties;
	}

	public static Properties getConfig(String configFileName) {
		Properties properties = new Properties();
		try {
			// properties.load(ClassLoader.getSystemResourceAsStream(configFileName));
			properties.load(ServerUtil.class.getClassLoader().getResourceAsStream(configFileName));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return properties;
	}
}
