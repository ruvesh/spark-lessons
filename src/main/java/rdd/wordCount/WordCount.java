package rdd.wordCount;

import common.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

public class WordCount {
    /*
     * Find number of occurrences of each word in the article in/word_count.text
     * Print output in console and set logger to log only errors
     * Get rid of the punctuations at the end of the words to make a cleaner word count
     * Also convert the words to lowercase
     */
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("word_count").setMaster("local[3]"));

        JavaRDD<String> words = sc.textFile("in/word_count.text")
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator()).map(word -> word.toLowerCase(Locale.ROOT))
                .map(word -> word.replaceAll(Utils.PUNCTUATIONS, ""));

        Map<String, Long> wordMap = words.countByValue();
        for(Map.Entry<String, Long> wordMapEntry : wordMap.entrySet()){
            System.out.println(wordMapEntry.getKey() + ": "+ wordMapEntry.getValue());
        }
    }
}
