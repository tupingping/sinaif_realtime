package com.sinaif.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {

    public static String format(Date date, String format) {
        return new SimpleDateFormat(format).format(date);
    }

    public static int getUnixTimestampFromyyyyMMdd(String yyyy_mm_dd) {
        try {
            Date d = new SimpleDateFormat("yyyy-MM-dd").parse(yyyy_mm_dd);
            return (int) (d.getTime() / 1000);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return unixTimestamp();
    }

    public static final Date fromUnixtimestamp(long unixtimestamp) {
        return new Date(unixtimestamp * 1000);
    }

    public static final int unixTimestamp() {
        return (int) (System.currentTimeMillis() / 1000);
    }

    public static final int unixTimestamp(String dataStr, String format) {
        try {
            Date d = new SimpleDateFormat(format).parse(dataStr);
            return (int) (d.getTime() / 1000);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return unixTimestamp();
    }

    public static final String yesterday() {
        return getTimeStringDate(unixTimestamp000() - 3600 * 24);
    }

    public static String getTimeStringDate(long linuxTimestamp) {
        return getTimeString(linuxTimestamp, "yyyy/MM/dd");
    }

    public static String getTimeString(long linuxTimestamp, String format) {
        return new SimpleDateFormat(format).format(new Date(linuxTimestamp * 1000));
    }

    public static int unixTimestamp000() {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return (int) (calendar.getTimeInMillis() / 1000);
    }

    public static long winTimestamp000() {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTimeInMillis();
    }

    public static int getTimestamp000(int dayNum) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        return (int) (calendar.getTimeInMillis() / 1000 - dayNum * 24 * 60 * 60);
    }

    /**
     * 获取UTC标准时
     * 格式 [yyyy-MM-dd'T'HH:mm:ss.sss'Z]
     *
     * @return
     */
    public static String getUTCDate(Date dateTime) {
        String dateStr = "";
        Date date = null;
        String months = "", days = "", hours = "", sec = "", minutes = "";
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'");
        StringBuffer UTCTimeBuffer = new StringBuffer();
        Calendar cal = Calendar.getInstance();
        cal.setTime(dateTime);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        int zoneOffset = cal.get(java.util.Calendar.ZONE_OFFSET);
        int dstOffset = cal.get(java.util.Calendar.DST_OFFSET);
        cal.add(java.util.Calendar.MILLISECOND, -(zoneOffset + dstOffset));
        int year = cal.get(Calendar.YEAR);
        int month = cal.get(Calendar.MONTH) + 1;
        int day = cal.get(Calendar.DAY_OF_MONTH);
        int hour = cal.get(Calendar.HOUR_OF_DAY);
        int minute = cal.get(Calendar.MINUTE);
        int second = cal.get(Calendar.SECOND);
        int millisecond = cal.get(Calendar.MILLISECOND);
        if (month < 10) {
            months = "0" + String.valueOf(month);
        } else {
            months = String.valueOf(month);
        }
        if (minute < 10) {
            minutes = "0" + String.valueOf(minute);
        } else {
            minutes = String.valueOf(minute);
        }
        if (day < 10) {
            days = "0" + String.valueOf(day);
        } else {
            days = String.valueOf(day);
        }
        if (hour < 10) {
            hours = "0" + String.valueOf(hour);
        } else {
            hours = String.valueOf(hour);
        }
        if (second < 10) {
            sec = "0" + String.valueOf(second);
        } else {
            sec = String.valueOf(second);
        }
        UTCTimeBuffer.append(year).append("-").append(months).append("-").append(days);
        UTCTimeBuffer.append("T").append(hours).append(":").append(minutes).append(":").append(sec).append(".").append(millisecond).append("Z");
        try {
            date = format.parse(UTCTimeBuffer.toString());
            dateStr = format.format(date);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dateStr;
    }

    /**
     * <pre>
     * 把时间按10分钟为区间截取，比如10:12:20，处理后为10:10:00
     * @author sam.xie
     * @date Feb 28, 2017 2:41:02 PM
     * @param date
     * @return
     */
    public static Date getDateTrim10min(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        int minute = cal.get(Calendar.MINUTE);
        int minuteTrim10 = 10 * (minute / 10);
        cal.set(Calendar.MINUTE, minuteTrim10);
        cal.set(Calendar.SECOND, 0);
        return cal.getTime();
    }

    /**
     * 当前日期字符串
     *
     * @return
     */
    public static final String currentDate() {
        return new SimpleDateFormat("yyyy/MM/dd").format(new Date());
    }

    public static final String currentDateTime() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
    }

    public static final Date now() {
        return Calendar.getInstance().getTime();
    }

    public static final String currentDate(String format) {
        return new SimpleDateFormat(format).format(new Date());
    }

    public static final String formatDate(Date date, String format) {
        return new SimpleDateFormat(format).format(date);
    }

    public static void main(String[] args) {
        System.out.println(unixTimestamp());
        Date date = fromUnixtimestamp(1488262913);
        System.out.println(date);
        System.out.println(getDateTrim10min(date));
        System.out.println(date);
    }
}
