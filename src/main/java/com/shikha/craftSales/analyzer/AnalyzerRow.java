package com.shikha.craftSales.analyzer;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.shikha.craftSales.CustomerRow;
import com.shikha.craftSales.SalesRow;

/**
 * Holds outcome of Analysis
 * state#year#month#day#hour#sales
 * 
 * @author shikha
 *
 */
public class AnalyzerRow implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3L;
	private long epoch;
	private String state;
	private String year="";
	private String month="";
	private String hour="";
	private String day="";
	private BigDecimal amount;
	
	
	public AnalyzerRow(CustomerRow c, SalesRow s) {
		this.state = c.getState();
		this.year = s.getYear();
		this.month = s.getMonth();
		this.hour = s.getHour();
		this.day = s.getDay();
		this.amount = s.getAmount();
		this.epoch = s.getEpoch() * 1000L;
	}
	public AnalyzerRow(String r, BigDecimal s) {
		this.state = r;
		this.amount = s;
		this.epoch = 0;
	}
	public AnalyzerRow(String r, String yr, BigDecimal s) {
		this.state = r;
		this.year = yr;
		this.amount = s;
		this.epoch = getTimestamp(yr, "01", "01", "00");
	}
	public AnalyzerRow(String r, String yr, String m, BigDecimal s) {
		this.state = r;
		this.year = yr;
		this.month=m;
		this.amount = s;
		this.epoch = getTimestamp(yr, m, "01", "00");
	}
	public AnalyzerRow(String r, String yr, String m, String d, BigDecimal s) {
		this.state = r;
		this.year = yr;
		this.month=m;this.day=d;
		this.amount = s;
		this.epoch = getTimestamp(yr, m, d, "00");
	}
	public AnalyzerRow(String r, String yr, String m, String d, String h, BigDecimal s) {
		this.state = r;
		this.year = yr;
		this.month=m;this.day=d;this.hour=h;
		this.amount = s;
		this.epoch = getTimestamp(yr, m, d, h);
	}
	
	private static long getTimestamp(String year, String month, String day, String hour) {
		String strDate = month +"/" + day + "/" + year + " " + hour;
		Long millis = 0L;
		try {
		    millis = new SimpleDateFormat("MM/dd/yyyy HH").parse(strDate).getTime();
		} catch (ParseException e) {
		    e.printStackTrace();
		}
		
		return millis;
	}
	
	public String getHour() {
		return hour;
	}

	public void setHour(String hour) {
		this.hour = hour;
	}
	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

	public String getDay() {
		return day;
	}

	public void setDay(String day) {
		this.day = day;
	}

	public BigDecimal getAmount() {
		return amount;
	}

	public void setAmount(BigDecimal amount) {
		this.amount = amount;
	}
	
	//Output String to store in HDFS
	public String toString() {
		return state + "#" + year + "#" + month + "#" + day + "#" + hour + "#" + amount.toString();
	}

	public long getEpoch() {
		return epoch;
	}

	public void setEpoch(long epoch) {
		this.epoch = epoch;
	}
}
