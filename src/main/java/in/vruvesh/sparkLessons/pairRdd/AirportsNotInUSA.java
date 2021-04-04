package in.vruvesh.sparkLessons.pairRdd;

import in.vruvesh.sparkLessons.common.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class AirportsNotInUSA {
    /*
     * Add Airport names and their country from the input file in/airports.text into PairRdds
     * filter out the airports which are in not in usa and write the key-value pairs to out/airports_not_in_usa.text
     */
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("airports-not-in-usa").setMaster("local[*]"));
        JavaRDD<String> airports = sc.textFile("in/airports.text");

        JavaPairRDD<String, String> airportNameAndCountryPair = airports.mapToPair(getAirportsByNameAndCountry());

        JavaPairRDD<String, String> airportsNotInUsa = airportNameAndCountryPair.filter(
                pair -> !pair._2().equals("\"United States\"")
        );

        airportsNotInUsa.saveAsTextFile("out/airports_not_in_usa.text");

    }

    private static PairFunction<String, String, String> getAirportsByNameAndCountry(){
        return line -> new Tuple2<>(line.split(Utils.COMMA_DELIMITER)[1], line.split(Utils.COMMA_DELIMITER)[3]);
    }
}
