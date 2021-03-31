package pairRdd.sort.wordCount;

import common.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Locale;

public class SortedWordCountByCount {
    /*
     *  Find Word Counts from file in/word_count.text
     *  Sort the result in descending order of the count
     */

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("sortedwordcountbycount").setMaster("local"));

        JavaRDD<String> lines = sc.textFile("in/word_count.text");
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> wordCountPair = words.mapToPair(
                word -> new Tuple2<>(word.toLowerCase(Locale.ROOT).replaceAll(Utils.PUNCTUATIONS, ""),
                        1)
        );
        JavaPairRDD<String, Integer> wordCountMapPair = wordCountPair.reduceByKey(
                (x,y) -> x+y
        );

        JavaPairRDD<Integer, String> countToWordPair = wordCountMapPair.mapToPair(
                wordToCount -> new Tuple2<>(wordToCount._2(), wordToCount._1())
        );

        JavaPairRDD<Integer, String> sortedByCount = countToWordPair.sortByKey(false);

        JavaPairRDD<String, Integer> finalResult = sortedByCount.mapToPair(
                countToWord -> new Tuple2<>(countToWord._2(), countToWord._1())
        );

        finalResult.saveAsTextFile("out/sortedWordCountByDescendingCount.text");
    }
}
