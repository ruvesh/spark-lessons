package in.vruvesh.sparkLessons.sparkSql;

import org.apache.log4j.Level;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.log4j.Logger;

import static org.apache.spark.sql.functions.*;

public class HousePriceProblem {
     /*
           * Create a Spark program to read the house data from in/RealEstate.csv,
             group by location, aggregate the average price per SQ Ft and max price, and sort by average price per SQ Ft.

     */

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession sparkSession = SparkSession.builder().appName("house-price-problem").master("local[1]").getOrCreate();
        DataFrameReader dataFrameReader = sparkSession.read();

        Dataset<Row> houseDetails = dataFrameReader.option("header", "true").csv("in/RealEstate.csv");
        Dataset<Row> castedHouseDetails = houseDetails.withColumn("Price SQ Ft", col("Price SQ Ft").cast("double"))
                .withColumn("Price", col("Price").cast("double"));

        castedHouseDetails.groupBy(col("Location"))
                .agg(avg(col("Price SQ Ft")), max(col("Price")))
                .orderBy(col("avg(Price SQ Ft)").desc()).show();
    }
}
