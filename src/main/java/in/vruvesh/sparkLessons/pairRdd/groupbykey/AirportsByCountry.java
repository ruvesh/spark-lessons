package in.vruvesh.sparkLessons.pairRdd.groupbykey;

import in.vruvesh.sparkLessons.common.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


public class AirportsByCountry {
    /*
     *  Group airports in the Airports file by Country
     */

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("AirportsGroupByCountry").setMaster("local[*]"));
        JavaRDD<String> lines = sc.textFile("in/airports.text");
        JavaPairRDD<String, String> airportsAndCountryPairs = lines.mapToPair(
              airport ->  new Tuple2<>(airport.split(Utils.COMMA_DELIMITER)[3], airport.split(Utils.COMMA_DELIMITER)[1])
        );

        JavaPairRDD<String, Iterable<String>>  airportsGroupedByCountry = airportsAndCountryPairs.groupByKey();
        airportsGroupedByCountry.saveAsTextFile("out/GroupedAirports.txt");
    }
}
