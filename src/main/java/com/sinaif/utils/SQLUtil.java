package com.sinaif.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLUtil {

	public static String mergeParams(String sql, Object... args) {
		int idx = 0;
		int start = 0;
		while(idx < sql.length()) {
			idx = sql.indexOf("?", start);
//			sql.
		}
		return sql;
	}
	
	public static void main(String[] args) {
		String sql = "insert into table(id, name) values (?, ?)";
		Pattern p = Pattern.compile("\\?");
		Matcher m = p.matcher(sql);
		while(m.find()) {
			System.out.println(m.group());
			System.out.println("start: " +  m.start());
		}
	}
}
