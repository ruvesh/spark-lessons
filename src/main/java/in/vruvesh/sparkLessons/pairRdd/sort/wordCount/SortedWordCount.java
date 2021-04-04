package in.vruvesh.sparkLessons.pairRdd.sort.wordCount;

import in.vruvesh.sparkLessons.common.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Locale;

public class SortedWordCount {
    /*
     *  Find Word Counts from file in/word_count.text
     *  Sort the result in descending order
     */
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("sortedwordcount"));

        JavaRDD<String> lines = sc.textFile("in/word_count.text");
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairRDD<String, Integer> wordPair = words.mapToPair(
                word -> new Tuple2<>(word.toLowerCase(Locale.ROOT).replaceAll(Utils.PUNCTUATIONS, ""),
                1)
        );
        JavaPairRDD<String, Integer> wordCountPair = wordPair.reduceByKey((x,y) -> x + y).sortByKey(false);
        wordCountPair.saveAsTextFile("out/sortedWordCount.text");
    }
}
