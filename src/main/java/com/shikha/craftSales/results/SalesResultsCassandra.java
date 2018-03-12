package com.shikha.craftSales.results;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import com.shikha.craftSales.StatesUtil;
import com.shikha.craftSales.results.ResultsRow;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

/**
 * Retrieving results data from Cassandra 
 * @author shikha
 *
 */
public class SalesResultsCassandra extends SalesResultsSimple {

	
	@Override
	public void results(JavaSparkContext sc, String outputDir) {
		JavaRDD<ResultsRow> resultsrdd = null;
		
		for (String st : StatesUtil.states) {
		   CassandraJavaRDD<ResultsRow> rdd 
			= javaFunctions(sc).cassandraTable("craft", "craftanalysis", mapRowTo(ResultsRow.class))
		                       .where("state=?", st);
		                       //.select(column("state"));
		   if (resultsrdd != null) {
			   //combining data for all the states
			   resultsrdd = resultsrdd.union(rdd);
		   } else {
			   resultsrdd = rdd;
		   }
		}
        
		//saving output into one file
		resultsrdd.repartition(1).saveAsTextFile(outputDir + "/_all_");
	}
}
