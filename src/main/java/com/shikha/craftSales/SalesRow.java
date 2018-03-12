package com.shikha.craftSales;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Represents Sales dataset Row
 * Sales information <timestamp,customer_id,sales_price>
 * @author shikha
 *
 */
public class SalesRow implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private long epoch;
	private String year;
	private String month;
	private String day;
	private String hour;
	private String customerId;
	private BigDecimal amount;
	
	
	public SalesRow (String epoch, String customerId, BigDecimal amount) {
		long ep = Long.valueOf(epoch);
		
		this.setEpoch(ep);
		//converting epoch to milliSecond for Date object
		Date epd = new Date(Long.parseLong(epoch) * 1000L);
		
		//Formating epoch to year, month, date, hour
		SimpleDateFormat f = new SimpleDateFormat("yyyy");
		this.year = f.format(epd);
		f = new SimpleDateFormat("MM");
		this.month = f.format(epd);
		f = new SimpleDateFormat("dd");
		this.day = f.format(epd);
		f = new SimpleDateFormat("HH");
		this.hour = f.format(epd);

		this.customerId = customerId;
		this.amount=amount;
	}
	
	/*
	 * Sales information <timestamp,customer_id,sales_price>;
	 * 1454313600#123#123456
	 */
	public static SalesRow parseLine(String s) {
		String[] t = s.split("#");
		
		return new SalesRow(t[0], t[1], BigDecimal.valueOf(Long.valueOf(t[2])));
	}

	public String getCustomerId() {
		return customerId;
	}
	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}
	public BigDecimal getAmount() {
		return amount;
	}
	public void setAmount(BigDecimal amount) {
		this.amount = amount;
	}

	public long getEpoch() {
		return epoch;
	}

	public void setEpoch(long epoch) {
		this.epoch = epoch;
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

	public String getHour() {
		return hour;
	}

	public void setHour(String hour) {
		this.hour = hour;
	}
	
	

}
