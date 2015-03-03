package com.github.ningg.flume.sink;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Used for extract info from RTX log.
 * 
 * @author Ning Guo
 *
 */

public class RTXRegex {

	private static final String THREAD_ID_REG = "^ThreadID=(\\d*)$";
	private static final String TIME_REG = "^Time=((\\d{4})-(\\d{2})-(\\d{2})\\s(\\d{2}):(\\d{2}):(\\d{2})$)";
	private static final String SENDER_REG = "^Sender=(.*)$";
	private static final String RECEIVERS_REG = "^Receivers=(.*)$";
	
	private static final Pattern THREAD_ID_PAT = Pattern.compile(THREAD_ID_REG);
	private static final Pattern TIME_PAT = Pattern.compile(TIME_REG);
	private static final Pattern SENDER_PAT = Pattern.compile(SENDER_REG);
	private static final Pattern RECEIVERS_PAT = Pattern.compile(RECEIVERS_REG);
	
	private static final String BLANK_STRING = "";
	
	private static Matcher THREAD_ID_MAT = THREAD_ID_PAT.matcher(BLANK_STRING);
	private static Matcher TIME_MAT = TIME_PAT.matcher(BLANK_STRING);
	private static Matcher SENDER_MAT = SENDER_PAT.matcher(BLANK_STRING);
	private static Matcher RECEVIERS_MAT = RECEIVERS_PAT.matcher(BLANK_STRING);

	public static final String RTX_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
	public static final String RTX_RECEIVER_SPLIT = ";";
	
	
	public static Matcher getThreadIdMat() {
		return THREAD_ID_MAT;
	}
	public static Matcher getTimeMat() {
		return TIME_MAT;
	}
	public static Matcher getSenderMat() {
		return SENDER_MAT;
	}
	public static Matcher getReceviersMat() {
		return RECEVIERS_MAT;
	}
	public static String dealWithDate(Matcher timeMat) {
		String year = timeMat.group(2);
		String month = timeMat.group(3);
		String day = timeMat.group(4);
		
		String hour = timeMat.group(5);
		String minute = timeMat.group(6);
		String second = timeMat.group(7);
		
		int monthValue = Integer.parseInt(month);
		int realMonthValue = monthValue + 1;
		String realMonth = (realMonthValue > 9) ? String.valueOf(realMonthValue) : "0" + String.valueOf(realMonthValue);
		
		String result = year + "-" +
						realMonth + "-" +
						day + " " +
						hour + ":" +
						minute + ":" +
						second + "";
		
		return result;
	}
	
}
