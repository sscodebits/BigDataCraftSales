package com.shikha.craftSales.analyzer;

import org.apache.spark.api.java.JavaRDD;


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
