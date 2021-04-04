package in.vruvesh.sparkLessons.sparkSql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;


import static org.apache.spark.sql.functions.*;


public class StackOverflowSurveyRelational {
    /*
     *  Load the file in/2016-stack-overflow-survey-responses.csv as a Relational Database Table
     *  Perform basic sql select operations
     *  Perform basic sql select with where clause operations
     *  Cast salary_midpoint and age_midpoint columns to integers
     *  Count number of responses from each occupation
     *  Find the average salaries and max age of the participants based on which country they are from
     *  Find the number of responses in each bucket of salary ranges in the interval of 20000 USD and sort results
     */

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession sparkSession = SparkSession.builder().appName("StackOverflowSurveySparkSQL").master("local[1]").getOrCreate();
        DataFrameReader dataFrameReader = sparkSession.read();

        Dataset<Row> responses = dataFrameReader.option("header", "true").csv("in/2016-stack-overflow-survey-responses.csv");
        responses.printSchema();
        responses.show(5);

        System.out.println("Select country and occupation");
        responses.select(col("country"), col("occupation")).show();

        System.out.println("Select occupation and self_identification with where condition on country");
        responses.select(col("occupation"), col("self_identification")).filter(col("country").equalTo("Albania")).show();

        System.out.println("Casting the age and salary midpoint values to integer values");
        Dataset<Row> castedResponse = responses.withColumn("age_midpoint", col("age_midpoint").cast("integer"))
                .withColumn("salary_midpoint", col("salary_midpoint").cast("integer"));

        castedResponse.printSchema();
        castedResponse.show(5);

        System.out.println("Count of responses grouped by occupation");
        RelationalGroupedDataset groupedByOccupation = responses.groupBy("occupation");
        groupedByOccupation.count().show();

        System.out.println("Average salaries and max age grouped by country");
        RelationalGroupedDataset groupedByCountry = responses.groupBy("country");
        groupedByCountry.agg(avg("salary_midpoint").as("average_salary"), max("age_midpoint").as("max_age"), count("*").as("no_of_responses")).show();

        System.out.println("Salary Range Problem");
        Dataset<Row> processedResponseWithSalaryRange = castedResponse.withColumn("salary_range", col("salary_midpoint").divide(20000).cast("integer").multiply(20000));

        System.out.println("Salary buckets");
        processedResponseWithSalaryRange.select("salary_midpoint", "salary_range").show();

        System.out.println("Count of responses of each salary range bucket");
        processedResponseWithSalaryRange.groupBy("salary_range").agg(count("*").as("no_of_responses")).orderBy(col("salary_range")).show();

        sparkSession.stop();
    }
}
