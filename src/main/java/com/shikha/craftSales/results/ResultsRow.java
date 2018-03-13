package com.shikha.craftSales.results;

import java.io.Serializable;
import java.math.BigDecimal;

import java.sql.Timestamp;


/**
 * Holder for results row
 * @author shikha
 *
 */
public class ResultsRow implements Serializable {
	private static final long serialVersionUID = 3L;
	private long epoch;
	private String state;
	private String year="";
	private String month="";
	private String hour="";
	private String day="";
	private BigDecimal amount;

	public ResultsRow() {}
	
	public ResultsRow(String state, String year, String month, String day, String hour, 
			BigDecimal amount,
			long epoch) {
		this.state = state;
		this.year = year;
		this.month=month;this.day=day;this.hour=hour;
		this.amount = amount;
		this.epoch = epoch;

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

	public String getHour() {
		return hour;
	}

	public void setHour(String hour) {
		this.hour = hour;
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

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

}
