package com.shikha.craftSales.analyzer;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 
 * @author shikha
 *
 */
public interface SalesAnalyzer {
	public void process(JavaSparkContext sc, JavaRDD<String> customers, 
			JavaRDD<String> sales, String outputDir);
}
