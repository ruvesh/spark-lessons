package rdd.sumOfNumbers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Int;

import java.util.Arrays;

public class SumOfPrimes {
    /*
     * Find sum of prime numbers in the file in/prime_nums.text
     * Print output to the console
     * Log only Errors and the output
     */
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("sum_of_primes").setMaster("local[1]"));

        JavaRDD<Integer> primes = sc.textFile("in/prime_nums.text")
                .flatMap(line -> Arrays.asList(line.split("\\s+")).iterator()).filter(nums -> !nums.isEmpty())
                .map(prime -> Integer.valueOf(prime));

        int sum = primes.reduce((x,y) -> x+y);
        System.out.println("Sum " + sum);
    }
}
