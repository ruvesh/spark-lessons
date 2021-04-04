package in.vruvesh.sparkLessons.pairRdd.sort;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import in.vruvesh.sparkLessons.pairRdd.AvgCount;
import scala.Tuple2;

public class AverageHousePriceSorted {
    /*
     *  Find the trend of increase in average house price as the number of bedrooms increase
     */

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("avghousepricesorted").setMaster("local[3]"));

        JavaRDD<String> lines = sc.textFile("in/RealEstate.csv");
        JavaRDD<String> cleanedLines = lines.filter(line -> !line.contains("Bedrooms"));

        JavaPairRDD<Integer, AvgCount> houseAvgCountPair = cleanedLines.mapToPair(
               line -> new Tuple2<>(Integer.parseInt(line.split(",")[3]),
                       new AvgCount(1, Double.parseDouble(line.split(",")[2])))
        );

        JavaPairRDD<Integer, AvgCount> housePriceTotal = houseAvgCountPair.reduceByKey(
                (x,y) -> new AvgCount(x.getCount() + y.getCount(), x.getTotal() + y.getTotal())
        );

        JavaPairRDD<Integer, Double> housePriceAvg = housePriceTotal.mapValues(
                avgCount -> avgCount.getTotal()/avgCount.getCount()
        );
        housePriceAvg.sortByKey().saveAsTextFile("out/sortedaverageprice.text");
    }
}
