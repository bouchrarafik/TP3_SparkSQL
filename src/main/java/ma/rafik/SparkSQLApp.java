package ma.rafik;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class SparkSQLApp {

    public static void main(String[] args) {

        String path = "resources/rentals.csv";

        SparkSession spark = SparkSession.builder()
                .appName("TP3 Spark SQL")
                .master("local[*]")
                .getOrCreate();


        // 1. DATA LOADING & EXPLORATION


        Dataset<Row> rentals = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv(path);

        System.out.println("\n===== SCHEMA =====");
        rentals.printSchema();

        System.out.println("\n===== FIRST 5 ROWS =====");
        rentals.show(5, false);

        System.out.println("\n===== TOTAL RENTALS =====");
        System.out.println(rentals.count());


        // 2. TEMP VIEW


        rentals.createOrReplaceTempView("bike_rentals_view");


        // 3. BASIC SQL QUERIES


        System.out.println("\n===== 3.1 RENTALS > 30 MIN =====");

        spark.sql("""
                SELECT *
                FROM bike_rentals_view
                WHERE duration_minutes > 30
                """).show(false);

        System.out.println("\n===== 3.2 RENTALS FROM STATION AEROPORT BUS =====");

        spark.sql("""
                SELECT *
                FROM bike_rentals_view
                WHERE start_station = 'Station Aeroport Bus'
                """).show(false);

        System.out.println("\n===== 3.3 TOTAL REVENUE =====");

        spark.sql("""
                SELECT ROUND(SUM(price),2) AS total_revenue
                FROM bike_rentals_view
                """).show(false);


        // 4. AGGREGATIONS


        System.out.println("\n===== 4.1 RENTALS PER START STATION =====");

        Dataset<Row> rentalsPerStation =
                rentals.groupBy("start_station").count();

        rentalsPerStation.show(false);

        System.out.println("\n===== 4.2 AVG DURATION PER STATION =====");

        rentals.groupBy("start_station")
                .avg("duration_minutes")
                .show(false);

        System.out.println("\n===== 4.3 MOST POPULAR STATION =====");

        rentalsPerStation
                .orderBy(col("count").desc())
                .show(1, false);


        // 5. TIME ANALYSIS


        System.out.println("\n===== 5.1 HOUR EXTRACTION =====");

        rentals.select(
                col("start_time"),
                hour(col("start_time")).alias("hour")
        ).show(false);

        System.out.println("\n===== 5.2 RENTALS PER HOUR =====");

        Dataset<Row> perHour =
                rentals.groupBy(hour(col("start_time")).alias("hour"))
                        .count();

        perHour.show(false);

        System.out.println("\n===== PEAK HOUR =====");

        perHour.orderBy(col("count").desc())
                .show(1, false);

        System.out.println("\n===== 5.3 MORNING STATION (7-12) =====");

        rentals.withColumn("hour", hour(col("start_time")))
                .filter(col("hour").between(7, 12))
                .groupBy("start_station")
                .count()
                .orderBy(col("count").desc())
                .show(1, false);


        // 6. USER ANALYSIS
       

        System.out.println("\n===== 6.1 AVG AGE =====");

        rentals.select(avg("age").alias("avg_age"))
                .show(false);

        System.out.println("\n===== 6.2 USERS BY GENDER =====");

        rentals.groupBy("gender")
                .count()
                .show(false);

        System.out.println("\n===== 6.3 AGE GROUPS =====");

        spark.sql("""
                SELECT
                    CASE
                        WHEN age BETWEEN 18 AND 30 THEN '18-30'
                        WHEN age BETWEEN 31 AND 40 THEN '31-40'
                        WHEN age BETWEEN 41 AND 50 THEN '41-50'
                        ELSE '51+'
                    END AS age_group,
                    COUNT(*) AS total
                FROM bike_rentals_view
                GROUP BY
                    CASE
                        WHEN age BETWEEN 18 AND 30 THEN '18-30'
                        WHEN age BETWEEN 31 AND 40 THEN '31-40'
                        WHEN age BETWEEN 41 AND 50 THEN '41-50'
                        ELSE '51+'
                    END
                ORDER BY total DESC
                """).show(false);

        spark.stop();
    }
}
