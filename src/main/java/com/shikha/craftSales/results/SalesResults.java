package com.shikha.craftSales.results;

import org.apache.spark.api.java.JavaSparkContext;

/**
 * 
 * @author shikha
 *
 */
public interface SalesResults {
	public void results(JavaSparkContext sc, String outputDir);
}
