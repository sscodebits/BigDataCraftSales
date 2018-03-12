package com.shikha.craftSales.results;

import org.apache.spark.api.java.JavaSparkContext;

/**
 * Retrieve results from HDFS
 * @author shikha
 *
 */
public class SalesResultsSimple implements SalesResults {
	public void results(JavaSparkContext sc, String outputDir) {};

}
