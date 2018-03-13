package com.shikha.craftSales;

import com.shikha.craftSales.analyzer.SalesAnalyzer;
import com.shikha.craftSales.analyzer.SalesAnalyzerCassandra;
import com.shikha.craftSales.analyzer.SalesAnalyzerCustSmall;
import com.shikha.craftSales.analyzer.SalesAnalyzerSimple;
import com.shikha.craftSales.results.SalesResults;
import com.shikha.craftSales.results.SalesResultsCassandra;
import com.shikha.craftSales.results.SalesResultsSimple;

/**
 * Creates a analyzer based on Strategy. By default creates Cassandra Analyzer
 * @author shikha
 *
 */
public class SalesAnalyzerFactory {
	public static enum STRATEGY {
		CUST_SMALL("cust_small"),
		SIMPLE("simple"),
		DEFAULT("default");
		
		private String val;
		private STRATEGY(String s) {
			val=s;
		}
		
		public static STRATEGY getStrategy (String s) {
			if (s == null)
				return DEFAULT;
			
			String strategy = s.trim().toLowerCase();
			if (strategy.equals(CUST_SMALL.val)) {
				return CUST_SMALL;
			} else if (strategy.equals(SIMPLE.val)) {
				return SIMPLE;
			} else {
				return DEFAULT;
			}
		}
	}
	
	
	public static SalesAnalyzer getAnalyzer(STRATEGY strategy) {
		
		switch (strategy) {
		case CUST_SMALL:
			return new SalesAnalyzerCustSmall();
		case SIMPLE:
			return new SalesAnalyzerSimple();
		default:
			return new SalesAnalyzerCassandra();
		}
	}
	
	public static SalesResults getResults(STRATEGY strategy) {
		switch (strategy) {
		case SIMPLE:
			return new SalesResultsSimple();
		default:
			return new SalesResultsCassandra();
		}
	}

}
