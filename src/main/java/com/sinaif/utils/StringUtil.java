package com.sinaif.utils;

import java.math.BigDecimal;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

public class StringUtil {

//  public static Gson gson = new Gson();
	public static Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

	public static JsonParser jsonParser = new JsonParser();

	public static final Calendar calendar = Calendar.getInstance(); // 多线程使用不安全

	private final static SimpleDateFormat yyyyMMdd = new SimpleDateFormat("yyyyMMdd");
	private final static SimpleDateFormat yyyyMMddHH = new SimpleDateFormat("yyyyMMddHH");
	private final static SimpleDateFormat yyyyMMddHHmm = new SimpleDateFormat("yyyyMMddHHmm");
	private final static SimpleDateFormat yyyyMMddHHmmss = new SimpleDateFormat("yyyyMMddHHmmss");
	private final static SimpleDateFormat yyyy_MM_ddHHmmss = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static int convertInt(String intStr, int defaultValue) {
		try {
			return Integer.valueOf(intStr).intValue();
		} catch (Throwable t1) {
			try {
				return Float.valueOf(intStr).intValue();
			} catch (Throwable t2) {
				return defaultValue;
			}
		}
	}

	public static long convertLong(String longStr, long defaultValue) {
		try {
			return Long.valueOf(longStr);
		} catch (Throwable t) {
			return defaultValue;
		}
	}

	public static float convertFloat(String floatStr, float defaultValue) {
		try {
			return Float.valueOf(floatStr);
		} catch (Throwable t) {
			return defaultValue;
		}
	}

	public static BigDecimal convertDecimal(String floatStr, float defaultValue) {
		try {
			return new BigDecimal(floatStr);
		} catch (Throwable t) {
			return BigDecimal.ZERO;
		}
	}

	public static double convertDouble(String doubleStr, double defaultValue) {
		try {
			return Double.valueOf(doubleStr);
		} catch (Throwable t) {
			return defaultValue;
		}
	}

	public static BigDecimal convertBigDecimal(String bigDecimalDStr, int defaultValue) {
		try {
			return new BigDecimal(bigDecimalDStr);
		} catch (Throwable t) {
			return new BigDecimal(defaultValue);
		}
	}

	public static BigDecimal convertBigDecimal(String bigDecimalDStr, int multiplier, int defaultValue) {
		try {
			return new BigDecimal(bigDecimalDStr).multiply(BigDecimal.valueOf(multiplier)).setScale(0,
					BigDecimal.ROUND_HALF_UP);
		} catch (Throwable t) {
			return new BigDecimal(defaultValue);
		}
	}

	public static BigDecimal convertBigDecimalWithDivide(String bigDecimalDStr, int multiplier, int defaultValue) {
		try {
			return new BigDecimal(bigDecimalDStr).divide(BigDecimal.valueOf(multiplier)).setScale(0,
					BigDecimal.ROUND_HALF_UP);
		} catch (Throwable t) {
			return new BigDecimal(defaultValue);
		}
	}

	public static String[] split(String line, String seperator) {
		if (line == null || seperator == null || seperator.length() == 0)
			return null;
		ArrayList<String> list = new ArrayList<String>();
		int pos1 = 0;
		int pos2;
		for (;;) {
			pos2 = line.indexOf(seperator, pos1);
			if (pos2 < 0) {
				list.add(line.substring(pos1));
				break;
			}
			list.add(line.substring(pos1, pos2));
			pos1 = pos2 + seperator.length();
		}
		// 去掉末尾的空串，和String.split行为保持一致
		for (int i = list.size() - 1; i >= 0 && list.get(i).length() == 0; --i) {
			list.remove(i);
		}
		return list.toArray(new String[0]);
	}

	public static String date2yyyyMMdd(long timemilles) {
		return yyyyMMdd.format(new Date(timemilles));
	}

	public static int date2yyyyMMddInt(long timemilles) {
		return StringUtil.convertInt(date2yyyyMMdd(timemilles), 0);
	}

	public static String date_yyyyMMddHHmm(long timemilles) {
		return yyyyMMddHHmm.format(new Date(timemilles));
	}

	public static Date yyyyMMddHH2Date(String str) throws ParseException {
		return yyyyMMddHH.parse(str);
	}

	public static Date yyyy_MM_ddHHmmss2Date(String str) throws ParseException {
		return yyyy_MM_ddHHmmss.parse(str);
	}

	public static String date2yyyy_MM_ddHHmmss(Date date) {
		return yyyy_MM_ddHHmmss.format(date);
	}

	public static String date_yyyyMMddHHmmss(long timemilles) {
		return yyyyMMddHHmmss.format(new Date(timemilles));
	}

	public static boolean isEmpty(String str) {
		if (null == str || "".equals(str.trim())) {
			return true;
		}
		return false;
	}

	public static String getJsonStr(Object o) {
		String str = gson.toJson(o);
		return str;
	}

	public static String getJsonElementStr(JsonObject json, String key) {
		return getJsonElementStr(json, key, null);
	}

	public static int getJsonElementInt(JsonObject json, String key) {
		return getJsonElementInt(json, key, -1);
	}
	
	public static String getJsonElementStr(JsonObject json, String key, String defaultVal) {
		try {
			return json.get(key).getAsString();
		}catch (Exception e) {
			// e.printStackTrace();
		}
		return defaultVal;
	}


	public static int getJsonElementInt(JsonObject json, String key, int defaultVal) {
		try {
			return json.get(key).getAsInt();
		}catch (Exception e) {
			// e.printStackTrace();
		}
		return defaultVal;
	}
	
	public static Map<String, String> getMapFromJson(String json) {
		try {
			return gson.fromJson(json, new TypeToken<Map<String, String>>() {
			}.getType());
		} catch (Throwable t) {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public static Set<String> getSetFromJson(String json) {
		try {
			return gson.fromJson(json, HashSet.class);
		} catch (Throwable t) {
			return null;
		}
	}

	public static <T1, T2> Map<T1, T2> getMapFromJson(String json, TypeToken<Map<T1, T2>> type) {
		try {
			return gson.fromJson(json, type.getType());
		} catch (Throwable t) {
			return null;
		}
	}

	public static <T1, T2> Map<T1, T2> getMapFromJson(String json, T1 O, T2 P) {
		try {
			return gson.fromJson(json, new TypeToken<Map<T1, T2>>() {
			}.getType());
		} catch (Throwable t) {
			return null;
		}
	}

	public static String convertEmptyStr(String src, String defaultValue) {
		if (isEmpty(src)) {
			return defaultValue;
		}
		return src;
	}

	/**
	 * 由于历史原因，原始日志中 appId 和 version 合在同一字段 通过 | 先分割，格式为 appId|appVersion
	 * 
	 * @param appIdAndVer
	 * @return
	 */
	public static String[] getAppIdAndVer(String appIdAndVer) {
		String[] arr = appIdAndVer.split("\\|");
		if (arr.length < 1) {
			arr = new String[] { "-", "-" };
		} else if (arr.length < 2) {
			arr = new String[] { arr[0], "-" };
		} else {
			if (isEmpty(arr[0])) {
				arr[0] = "-";
			}
			if (isEmpty(arr[1])) {
				arr[1] = "-";
			}
		}
		return arr;
	}

	/**
	 * 将unixstamp时间戳转换为 去掉时分秒的整天时间戳 线程不安全
	 */
	public static int truncateDate(int unixTimestamp, int defaultValue) {
		calendar.setTimeInMillis(unixTimestamp * 1000L);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		return (int) (calendar.getTimeInMillis() / 1000);
	}

	public static String getJsonStrValue(JsonObject obj, String member) {
		try {
			return obj.get(member).getAsString();
		} catch (Exception e) {
			return "";
		}
	}

	public static int getJsonIntValue(JsonObject obj, String member) {
		try {
			return obj.get(member).getAsInt();
		} catch (Exception e) {
			return 0;
		}
	}

	public static long getJsonLongValue(JsonObject obj, String member) {
		try {
			return obj.get(member).getAsLong();
		} catch (Exception e) {
			return 0;
		}
	}

	public static float getJsonFloatValue(JsonObject obj, String member) {
		try {
			return obj.get(member).getAsFloat();
		} catch (Exception e) {
			return 0;
		}
	}

	public static double getJsonDoubleValue(JsonObject obj, String member) {
		try {
			return obj.get(member).getAsFloat();
		} catch (Exception e) {
			return 0;
		}
	}

	/**
	 * 将unixstamp时间戳转换为 去掉时分秒的整天时间戳 线程不安全
	 */
	public static int truncateDate(String unixTimestamp, int defaultValue) {
		return truncateDate(StringUtil.convertInt(unixTimestamp, 0), defaultValue);
	}

	/**
	 * <pre>
	 * 从appId和version混合字段中分割出appId
	 * 
	 * @param appIdAndVer
	 * @return
	 */
	public static String getRealAppId(String appIdAndVer) {
		return getAppIdAndVer(appIdAndVer)[0];
	}

	/**
	 * <pre>
	 * 将字符串泛型类型的集合以特定的分隔符拼接
	 *
	 * @param separator
	 * @return
	 */
	public static String join(Collection<String> collection, String separator) {
		StringBuilder sb = new StringBuilder();
		for (String item : collection) {
			sb.append(item).append(separator);
		}
		// 去掉最后的分隔符
		if (sb.length() == 0) {
			return "";
		} else {
			return sb.substring(0, sb.length() - separator.length());
		}
	}

	public static String join(String[] arr, String separator) {
		StringBuilder sb = new StringBuilder();
		for (String item : arr) {
			sb.append(item).append(separator);
		}
		// 去掉最后的分隔符
		if (sb.length() == 0) {
			return "";
		} else {
			return sb.substring(0, sb.length() - separator.length());
		}
	}

	public static String join(Integer[] arr, String separator) {
		StringBuilder sb = new StringBuilder();
		for (Integer item : arr) {
			sb.append(item).append(separator);
		}
		// 去掉最后的分隔符
		if (sb.length() == 0) {
			return "";
		} else {
			return sb.substring(0, sb.length() - separator.length());
		}
	}

	public static String getFloatString(float value, int scale) {
		BigDecimal bd = new BigDecimal(value + "");
		bd = bd.setScale(scale, BigDecimal.ROUND_HALF_UP);
		return bd.toString();
	}

	public static String[] merge(String[]... arrays) {
		int length = 0;
		for (String[] array : arrays) {
			length += array.length;
		}
		String[] mergeArray = new String[length];
		int i = 0;
		for (String[] array : arrays) {
			for (String item : array) {
				mergeArray[i++] = item;
			}
			length += array.length;
		}
		return mergeArray;
	}

	// 将使用逗号连接的字符串，转换为Set
	public static Set<String> parseStr2Set(String tags) {
		Set<String> set = new HashSet<String>();
		String[] tagArr = tags.split(",");
		if (tagArr.length > 0) {
			for (String tag : tagArr) {
				set.add(tag);
			}
		}
		return set;
	}

	// 将01001类似的字符串，转换为int型的bit位表示，每30个字符使用1个int表示
	public static String parseBitList2IntList(String bitList) {
		// 每30位作为一个步长
		char[] bits = bitList.toCharArray();
		int size = bits.length % 30 == 0 ? bits.length / 30 : bits.length / 30 + 1;
		Integer[] bitInt = new Integer[size];
		int firstStep = bits.length - (size - 1) * 30;
		for (int i = 0; i < size; i++) {
			int startIndex = 0;
			int endIndex = firstStep;
			if (i > 0) {
				endIndex = firstStep + i * 30;
				startIndex = endIndex - 30;
			}
			int offset = 0;
			bitInt[i] = 0;
			for (int j = endIndex - 1; j >= startIndex; j--) {
				if (bits[j] == '1') {
					bitInt[i] = bitInt[i] | (1 << offset);
				}
				offset++;
			}
		}
		return StringUtil.join(bitInt, ",");
	}

	public static String[] convertMapValue2Array(Map<String, String> map) {
		if (null != map && map.size() > 0) {
			String[] result = new String[map.size()];
			int i = 0;
			Iterator<String> iter = map.values().iterator();
			while (iter.hasNext()) {
				result[i++] = iter.next();
			}
			return result;
		}
		return null;
	}

	public static String convertUnicode2String(String unicodeStr) {
		StringBuilder sb = new StringBuilder();
		int i = -1;
		int pos = 0;
		while ((i = unicodeStr.indexOf("\\u", pos)) != -1) {
			sb.append(unicodeStr.substring(pos, i));
			if (i + 5 < unicodeStr.length()) {
				pos = i + 6;
				sb.append((char) Integer.parseInt(unicodeStr.substring(i + 2, i + 6), 16));
			}
		}
		sb.append(unicodeStr.substring(pos));
		return sb.toString();
	}

    /**
     * url转码
     *
     * @param url
     * @return
     */
    public static String urlEncode(String url) {
        try {
            return URLEncoder.encode(url, "utf-8");
        } catch (Exception e) {
            return null;
        }
    }

    public static String urlDecode(String url) {
        try {
            return URLDecoder.decode(url, "utf-8");
        } catch (Exception e) {
            return null;
        }
    }
    
	public static void main(String[] args) {
		String unicode = "{\"ret\":0,\"data\":{\"accounts\":[{\"app_id\":0,\"account_name\":\"\\u73b0  \n \\u91d1\\u8d26\\u6237\",\"balance\":3504726,\"daily_cost\":0,\"account_function\":1},{\"app_id\":1,\"account_name\":\"\\u865a\\u62df\\u8d26\\u6237\",\"balance\":0,\"daily_cost\":0,\"account_function\":1}],\"invoice_accounts\":[{\"app_id\":0,\"account_name\":\"\\u73b0\\u91d1\\u8d26\\u6237\"},{\"app_id\":1,\"account_name\":\"\\u865a\\u62df\\u8d26\\u6237\"}],\"day_budget\":1000000,\"daily_cost\":0,\"reach_day_budget\":\"\",\"day_budget_alarm_flag\":\"\",\"fund_not_enough\":\"\"}}  ";
		String jsonString = "{\"info\":\"{\"id\":12, \"name\":\"sara\"}\"}";
		System.out.println(jsonString);
		StringUtil.jsonParser.parse(unicode);
		System.out.println(convertUnicode2String(unicode));
		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd hh:mm:ss").create();
		System.out.println(gson.toJson(new Date()));

	}
}
