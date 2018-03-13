package com.shikha.craftSales;

import java.io.Serializable;

/**
 * Represents Customer dataset data row
 * Customer information <customer_id,name,street,city,state,zip>
 * @author shikha
 *
 */
public class CustomerRow  implements Serializable 
{
	
	private static final long serialVersionUID = 2L;
	private String id;
	private String name;
	private String addr;
	private String state;
	private String zip;
	
	public CustomerRow (String id, String name, String addr,
			String state, String zip) {
		this.id = id;
		this.name = name;
		this.addr=addr;
		this.state=state;
		this.zip=zip;
	}
	
	/*
	 * Customer information <customer_id,name,street,city,state,zip>
	 * Sample input line: 123#AAA Inc#1 First Ave  Mountain View CA#94040
	 */
	public static CustomerRow parseLine(String s) {
		String[] t = s.split("#");
		// get last two chars from address field 
		String t2 = t[2].trim();
		String state = t2.substring(t2.length() - 2);
		//String[]  addrArray = t2.split("\t");
		//String state = addrArray[2];
		return new CustomerRow(t[0], t[1],t[2],state,t[3]);
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getAddr() {
		return addr;
	}
	public void setAddr(String addr) {
		this.addr = addr;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	public String getZip() {
		return zip;
	}
	public void setZip(String zip) {
		this.zip = zip;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	
	
}
