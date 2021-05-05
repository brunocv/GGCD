package ggcd;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Rating_Year {

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

        Dataset<Row> ds2 = spark.read()
                .option("header","true")
                .option("delimiter","\t")
                .csv("/home/dreamerz/Desktop/GGCD/Dados/original/title.ratings.tsv.gz");

        ds2.printSchema();

        ds2.createOrReplaceTempView("title_ratings");
        spark.sql("select tb1.startYear, tr1.averageRating as averageRating, tb1.primaryTitle " +
                        "from title_basics as tb1 join title_ratings as tr1 on tb1.tconst = tr1.tconst " +
                        "join ( select tb.startYear as year, max(tr.averageRating) as averageRating " +
                        "from title_basics as tb join title_ratings as tr on tb.tconst = tr.tconst where tb.titleType= 'movie' " +
                        "group by tb.startYear) b on b.year= tb1.startYear and b.averageRating= tr1.averageRating " +
                        "where tb1.titleType= 'movie' order by tb1.startYear")
                        .coalesce(1).write().option("header","true").format("csv").save("result");


    }

}
