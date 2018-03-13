package com.shikha.craftSales.analyzer;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;

/**
 * Extends Base to change the calculation of different statistics for optimization.
 * JoinData is only used once now to calculate the Hourly stats. Then, this result is used for Day stats and so on.
 * @author shikha
 *
 */
public class SalesAnalyzerSimple extends SalesAnalyzerBase {

	private static final long serialVersionUID = 4838140167051382277L;

	@Override
	protected void processJoinedData(JavaRDD<AnalyzerRow> joinData, String outputDir) {

		//calculating total sales based on state, year, month, day, Hour
		JavaRDD<AnalyzerRow> sumHour
	    = joinData.mapToPair(j -> new Tuple2<>(new Tuple5<>(j.getState(), j.getYear(), j.getMonth(), j.getDay(), j.getHour()), j.getAmount()))
	      .reduceByKey( (x,y) -> x.add(y))
	      .map(j2 -> new AnalyzerRow(j2._1._1(), j2._1._2(), j2._1._3(), j2._1._4(), j2._1._5(), j2._2));
		
		storeRDD(sumHour, outputDir + "/h");

		//calculating total sales based on state, year, month, day
		JavaRDD<AnalyzerRow> sumDay 
		= sumHour.mapToPair(j -> new Tuple2<>(new Tuple4<>(j.getState(), j.getYear(), j.getMonth(), j.getDay()), j.getAmount()))
	      .reduceByKey( (x,y) -> x.add(y))
	      .map(j2 -> new AnalyzerRow(j2._1._1(), j2._1._2(), j2._1._3(), j2._1._4(), j2._2));
		
		storeRDD(sumDay, outputDir + "/d");
		
		//calculating total sales based on state, year, month
		JavaRDD<AnalyzerRow> sumMonth
	    = sumDay.mapToPair(j -> new Tuple2<>(new Tuple3<>(j.getState(), j.getYear(), j.getMonth()), j.getAmount()))
	      .reduceByKey( (x,y) -> x.add(y))
	      .map(j2 -> new AnalyzerRow(j2._1._1(), j2._1._2(), j2._1._3(), j2._2));
		
		storeRDD(sumMonth, outputDir + "/m");
		
		//calculating total sales based on state and year
		JavaRDD<AnalyzerRow> sumYear
	    = sumMonth.mapToPair(j -> new Tuple2<>(new Tuple2<>(j.getState(), j.getYear()), j.getAmount()))
	      .reduceByKey( (x,y) -> x.add(y))
	      .map(j2 -> new AnalyzerRow(j2._1._1, j2._1._2, j2._2));
		
		storeRDD(sumYear, outputDir + "/y");
		
		//calculating total sales based on State
		JavaRDD<AnalyzerRow> sumState
		    = sumYear.mapToPair(j -> new Tuple2<>(j.getState(), j.getAmount()))
		      .reduceByKey( (x,y) -> x.add(y))
		      .map(j2 -> new AnalyzerRow(j2._1, j2._2));
		
		storeRDD(sumState, outputDir + "/st");

//		List<AnalyzerRow> rows = joinData.collect();
//		for (AnalyzerRow row : rows) {
//			System.out.println(row.toString());
//		}

	}
}
