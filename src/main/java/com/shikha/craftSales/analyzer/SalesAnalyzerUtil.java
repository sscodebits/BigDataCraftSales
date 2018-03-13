package com.shikha.craftSales.analyzer;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.shikha.craftSales.CustomerRow;
import com.shikha.craftSales.SalesRow;

import scala.Tuple2;

/**
 * This is Utility class to load and save the input and output Files
 * @author shikha
 *
 */
public class SalesAnalyzerUtil {
	public static void saveFile(JavaPairRDD<String, Tuple2<CustomerRow, SalesRow>> output, String outputDir) {
		output.take(10).forEach(x -> System.out.println(x));
		output.saveAsTextFile(outputDir);
	}
	public static JavaRDD<String> loadFile(JavaSparkContext sc, String file) {
		// Load input data.
		JavaRDD<String> input = sc.textFile(file);
		return input;
	}

}
