package com.shikha.craftSales.analyzer;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.shikha.craftSales.CustomerRow;
import com.shikha.craftSales.SalesRow;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;

/**
 * Performs analysis based on simple logic. It creates the CustomerRow and SalesRow RDDs by parsing the input line.
 * It joins the two RDDS using CustomerId as key
 * It calculates the sum for each granularities using reduceByKey
 *  <state,total_sales> for (year, month, day and hour) granularities
 *  
 * @author shikha
 *
 */
public class SalesAnalyzerBase implements SalesAnalyzer, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2788325665486397559L;

	@Override
	public void process(JavaSparkContext sc, JavaRDD<String> customers, 
			JavaRDD<String> sales, String outputDir) {

		// parse the lines and create CustomerRow object RDD
		JavaRDD<CustomerRow> customerRows = customers.map(line -> CustomerRow.parseLine(line));
		
		// parse the lines and create SalesRow object RDD
		JavaRDD<SalesRow> salesRows = sales.map(line -> SalesRow.parseLine(line));

		//Creates a pair RDD using customerId as Key and value as CustomerRow for joining 		
		JavaPairRDD<String, CustomerRow> cRows = customerRows
				.mapToPair(c -> new Tuple2<>(c.getId(), c));
		
		//Creates a pair RDD using customerId as Key and value as SalesRow for joining 		
		JavaPairRDD<String, SalesRow> sRows = salesRows
				.mapToPair(s -> new Tuple2<>(s.getCustomerId(), s));

		JavaRDD<AnalyzerRow> joinData = join(sc, cRows, sRows);
		processJoinedData(joinData, outputDir);
	}
	
	
	protected void processJoinedData(JavaRDD<AnalyzerRow> joinData, String outputDir) {
		// cache the results for multiple usages
		joinData.cache();
		
		//calculating total sales based on State
		JavaRDD<AnalyzerRow> sumState
		    = joinData.mapToPair(j -> new Tuple2<>(j.getState(), j.getAmount()))
		      .reduceByKey( (x,y) -> x.add(y))
		      .map(j2 -> new AnalyzerRow(j2._1, j2._2));
		
		storeRDD(sumState, outputDir + "/st");
		
		//calculating total sales based on state and year
		JavaRDD<AnalyzerRow> sumYear
	    = joinData.mapToPair(j -> new Tuple2<>(new Tuple2<>(j.getState(), j.getYear()), j.getAmount()))
	      .reduceByKey( (x,y) -> x.add(y))
	      .map(j2 -> new AnalyzerRow(j2._1._1, j2._1._2, j2._2));
		
		storeRDD(sumYear, outputDir + "/y");
		
		//calculating total sales based on state, year, month
		JavaRDD<AnalyzerRow> sumMonth
	    = joinData.mapToPair(j -> new Tuple2<>(new Tuple3<>(j.getState(), j.getYear(), j.getMonth()), j.getAmount()))
	      .reduceByKey( (x,y) -> x.add(y))
	      .map(j2 -> new AnalyzerRow(j2._1._1(), j2._1._2(), j2._1._3(), j2._2));
		
		storeRDD(sumMonth, outputDir + "/m");

		//calculating total sales based on state, year, month, day
		JavaRDD<AnalyzerRow> sumDay
	    = joinData.mapToPair(j -> new Tuple2<>(new Tuple4<>(j.getState(), j.getYear(), j.getMonth(), j.getDay()), j.getAmount()))
	      .reduceByKey( (x,y) -> x.add(y))
	      .map(j2 -> new AnalyzerRow(j2._1._1(), j2._1._2(), j2._1._3(), j2._1._4(), j2._2));
		
		storeRDD(sumDay, outputDir + "/d");

		//calculating total sales based on state, year, month, day, Hour
		JavaRDD<AnalyzerRow> sumHour
	    = joinData.mapToPair(j -> new Tuple2<>(new Tuple5<>(j.getState(), j.getYear(), j.getMonth(), j.getDay(), j.getHour()), j.getAmount()))
	      .reduceByKey( (x,y) -> x.add(y))
	      .map(j2 -> new AnalyzerRow(j2._1._1(), j2._1._2(), j2._1._3(), j2._1._4(), j2._1._5(), j2._2));
		
		storeRDD(sumHour, outputDir + "/h");

		
//		sumState.union(sumYear)
//		   .union(sumMonth)
//		   .union(sumDay)
//		   .union(sumHour)
//		   .saveAsTextFile(outputDir + "/all");

		//saveFile(joinData, outputDir);
		
	}
	
	/**
	 * Join the input data
	 * @param cRows
	 * @param sRows
	 * @return
	 */
	protected JavaRDD<AnalyzerRow> join(JavaSparkContext sc, JavaPairRDD<String, CustomerRow> cRows, JavaPairRDD<String, SalesRow> sRows) {
		//Tuple2<String, Tuple2<Customer,Sales>>
		JavaRDD<AnalyzerRow> joinData = cRows.join(sRows).map(x -> new AnalyzerRow(x._2._1, x._2._2));
		return joinData;
	}
	
	public void storeRDD(JavaRDD<AnalyzerRow> sumData, String path) {
		sumData.saveAsTextFile(path);
	}

}
