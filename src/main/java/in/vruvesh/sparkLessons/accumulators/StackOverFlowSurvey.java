package in.vruvesh.sparkLessons.accumulators;

import in.vruvesh.sparkLessons.common.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import scala.Option;



public class StackOverFlowSurvey {

    /*
     *  Count of responses from Canada
     *  Total number of records in the stackoverflow survey file of 2016
     *  Number of records with missing salary midpoints in entire survey
     *  Number of records with missing salary midpoints in survey data from Canada
     *  Print out all responses from Canada
     *  Print total number of bytes processed during the entire operation
     */

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf sparkConf = new SparkConf().setAppName("StackOverflowSurvey").setMaster("local[1]");
        SparkContext sparkContext = new SparkContext(sparkConf);
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);

        JavaRDD<String> surveyData = javaSparkContext.textFile("in/2016-stack-overflow-survey-responses.csv")
                .filter(line -> !line.contains("country"));


        final LongAccumulator total = new LongAccumulator();
        final LongAccumulator missingSalaryMidpoint = new LongAccumulator();
        final LongAccumulator missingSalaryMidpointInCanada = new LongAccumulator();
        final LongAccumulator processedBytes = new LongAccumulator();

        total.register(sparkContext, Option.apply("total"), false);
        missingSalaryMidpoint.register(sparkContext, Option.apply("missing salary middle point"), false);
        missingSalaryMidpointInCanada.register(sparkContext, Option.apply("missing salary middle point in canada data"), false);
        processedBytes.register(sparkContext, Option.apply("Processed Bytes"), true);

        JavaRDD<String> surveyDataFromCanada = surveyData.filter(response -> {
            String[] splits = response.split(Utils.COMMA_DELIMITER, -1);
            total.add(1);
            processedBytes.add(response.getBytes().length);

            if (splits[14].isEmpty()) {
                if(splits[2].equals("Canada"))
                    missingSalaryMidpointInCanada.add(1);
                missingSalaryMidpoint.add(1);
            }


            return splits[2].equals("Canada");

        });


        System.out.println("Respones from Canada: " + surveyDataFromCanada.count());
        System.out.println("Total number of responses: " + total.value());
        System.out.println("Responses with missing salary midpoint: " + missingSalaryMidpoint.value());
        System.out.println("Responses with missing salary midpoint in Canada: " + missingSalaryMidpointInCanada.value());
        System.out.println("-----------Survey Data from Canada----------------");
        surveyDataFromCanada.collect().forEach(System.out::println);
        System.out.println("--------------------------------------------------");
        System.out.println("Total Bytes processed in entire operation: " + processedBytes.value());
    }
}
