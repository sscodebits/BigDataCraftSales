package com.shikha.craftSales.analyzer;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import com.shikha.craftSales.CustomerRow;
import com.shikha.craftSales.SalesRow;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

public class SalesAnalyzerCassandra extends SalesAnalyzerSimple {

	/**
	 * Saves Analyzer results to Cassandra
	 */
	@Override
	public void storeRDD(JavaRDD<AnalyzerRow> sumData, String path) {
		javaFunctions(sumData).writerBuilder("craft", "craftanalysis", mapToRow(AnalyzerRow.class)).saveToCassandra();
	}
	

}
