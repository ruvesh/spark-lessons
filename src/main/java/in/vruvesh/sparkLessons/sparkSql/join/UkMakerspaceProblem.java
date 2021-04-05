package in.vruvesh.sparkLessons.sparkSql.join;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class UkMakerspaceProblem {
    /*
     *  Find the distribution of makerspaces accross the different regions in the UK
     *  Find the count of makerspaces in each region in the UK
     */
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("makerspaces-distribution").master("local[1]").getOrCreate();
        DataFrameReader reader = session.read();

        Dataset<Row> makerSpaces = reader.option("header", "true").csv("in/uk-makerspaces-identifiable-data.csv");
        // concat whitespace after the postcode prefix in the postcode dataset to avoid matching in cases where
        // the postcode matches with two prefixes whose first few characters are same
        Dataset<Row> postCodes = reader.option("header", "true").csv("in/uk-postcode.csv")
                .withColumn("Postcode", concat_ws("", col("Postcode"), lit(" ")));

        Dataset<Row> resultSet = makerSpaces.join(postCodes, makerSpaces.col("Postcode").startsWith(postCodes.col("Postcode")), "leftouter");
        System.out.println("Regions that each makerspace belongs to in the UK");
        resultSet.select(col("Name of Makerspace"), col("Region")).show();
        System.out.println("Number of makerspaces in each region in the UK");
        resultSet.groupBy(col("Region")).agg(count("*").as("Number of Makerspaces")).show();

    }
}
