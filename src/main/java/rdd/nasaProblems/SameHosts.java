package rdd.nasaProblems;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SameHosts {
    public static void main(String[] args) {
        /*
         * find the same hosts that were accessed on both the days logged in nasa apache web log files
         */
        SparkConf conf = new SparkConf().setAppName("nasa_same_hosts").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> nasaHostsDay1 = sc.textFile("in/nasa_19950701.tsv")
                .map(line -> line.split("\t")[0]);
        JavaRDD<String> nasaHostsDay2 = sc.textFile("in/nasa_19950801.tsv")
                .map(line -> line.split("\t")[0]);

        JavaRDD<String> sameHosts = nasaHostsDay1.intersection(nasaHostsDay2)
                .filter(host -> !host.equals("host"));

        sameHosts.saveAsTextFile("out/nasa_logs_same_hosts.csv");
    }
}
