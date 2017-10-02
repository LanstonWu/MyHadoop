package com.sywu.test;

public class TemperatureParseTest {
	public static void main(String[] args) {
		String s = "035623 99999  19720101    39.0 24    33.3 24  9999.9  0  9999.9  0    4.9 24   10.1 24   15.9  999.9    41.0*   37.4* 99.99  999.9  010000";
		String year = s.substring(14, 20);
		String min = s.substring(111, 116);
		String max = s.substring(25,30);
		System.out.println(year + "," + min + "," + max);

		System.out.println(max.matches("^([^A-Za-z]*?[A-Z][A-Za-z]*?)+.?"));
		if (max.matches("\\d{0,30}(.)\\d{0,30}")) {
			double maxVal = Double.parseDouble(max);
			System.out.println("val:" + maxVal);
		}
System.out.println(Double.max(Double.MIN_VALUE,41.0));
	}
}