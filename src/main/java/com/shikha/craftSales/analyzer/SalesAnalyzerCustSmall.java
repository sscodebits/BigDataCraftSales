package com.shikha.craftSales.analyzer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.shikha.craftSales.CustomerRow;
import com.shikha.craftSales.SalesRow;

import scala.Tuple2;

public class SalesAnalyzerCustSmall extends SalesAnalyzerCassandra {
	
	/**
	 * Implement Broadcast hash join with Customer as small data set
	 * See page 80 - High Performance Spark book
	 */
	@Override
	protected JavaRDD<AnalyzerRow> join(JavaSparkContext sc, JavaPairRDD<String, CustomerRow> cRows, JavaPairRDD<String, SalesRow> sRows) {
		// collect the customer to the driver as map and broadcast
		Map<String, CustomerRow> cRowsMap = cRows.collectAsMap();
		sc.broadcast(cRowsMap);
		
		// Use mapPartitions to combine the elements of sRows with the broadcast of customer Rows
		JavaRDD<AnalyzerRow> joinedRow = sRows.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,SalesRow>>, AnalyzerRow>() {
            @Override
            public Iterator<AnalyzerRow> call(Iterator<Tuple2<String,SalesRow>> salesPerPartition) throws Exception {
                List<AnalyzerRow> joinedRows = new ArrayList<>();
                while (salesPerPartition.hasNext()) {
                	Tuple2<String,SalesRow> salesRow = salesPerPartition.next();
                	CustomerRow cust = cRowsMap.get(salesRow._1);
                    joinedRows.add(new AnalyzerRow(cust, salesRow._2()));
                }
                return joinedRows.iterator();
            }
        });
		
		return joinedRow;
	}

}
