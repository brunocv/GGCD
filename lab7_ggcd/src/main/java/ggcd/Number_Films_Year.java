package ggcd;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Number_Films_Year {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("exe1")
                .master("local")
                .getOrCreate();

        Dataset<Row> ds = spark.read()
                .option("header","true")
                .option("delimiter","\t")
                .csv("/home/dreamerz/Desktop/GGCD/Dados/original/title.basics.tsv.gz");

        ds.printSchema();

        ds.createOrReplaceTempView("title_basics");

        spark.sql("select startYear, count(startYear) from title_basics where titleType= 'movie' group by startYear order by startYear").show(1000);


    }



}
