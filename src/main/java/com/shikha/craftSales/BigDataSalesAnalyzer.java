package com.shikha.craftSales;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.shikha.craftSales.SalesAnalyzerFactory.STRATEGY;
import com.shikha.craftSales.analyzer.SalesAnalyzer;
import com.shikha.craftSales.analyzer.SalesAnalyzerUtil;
import com.shikha.craftSales.results.SalesResults;


/**
 * This is main class to invoke the Spark job. It takes path of output directory and location of input datasets
 * 
 * @author shikha
 *
 */
public class BigDataSalesAnalyzer {
	boolean _init = false;
	
	public JavaSparkContext init(String[] args) {
		if (args.length < 5 ) {
			System.err.println(
					"Usage: BigDataSalesAnalyzer master outputDirHDFS customer sales strategy");
			System.exit(1);
		}

		_init = true;

		// Create a Java Spark Context
		JavaSparkContext sc = createSparkContext(args[0]);
		return sc;
	}
	
	public void process(String[] args) {
		JavaSparkContext sc = init(args);
		
			String outputDir = args[1];

			JavaRDD<String> customer = SalesAnalyzerUtil.loadFile(sc, args[2]);
			JavaRDD<String> sales = SalesAnalyzerUtil.loadFile(sc, args[3]);
			

		if (!_init) {
			System.err.println("Analyzer not initialized");
			return;
		}
		
		STRATEGY strategy = STRATEGY.getStrategy(args[4]);
		SalesAnalyzer analyzer = SalesAnalyzerFactory.getAnalyzer(strategy);
		analyzer.process(sc, customer, sales, outputDir);
		SalesResults results = SalesAnalyzerFactory.getResults(strategy);
		results.results(sc, outputDir);

	}	


	private JavaSparkContext createSparkContext(String master) {
		SparkConf conf = new SparkConf().setMaster(master).setAppName("Craft Sales Analyzer");
		conf.set("spark.cassandra.connection.host", "localhost");
		// overwrite output directory
		conf.set("spark.hadoop.validateOutputSpecs", "false");
		JavaSparkContext sc = new JavaSparkContext(conf);
		return sc;
	}

	public static void main(String[] args) {
		BigDataSalesAnalyzer analyzer = new BigDataSalesAnalyzer();
			analyzer.process(args);
		
	}

}
