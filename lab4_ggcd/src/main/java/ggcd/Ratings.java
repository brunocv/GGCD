package ggcd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

public class Ratings {

    ///home/bruno/Desktop/GGCD/Dados/mini

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("g0spark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Double> mr = sc.textFile("/home/bruno/Desktop/GGCD/Dados/mini/title.ratings.tsv.bz2")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .filter(l -> !l.equals("\\N"))
                .filter(l -> Double.parseDouble(l[1]) >= 9.0)
                .mapToPair(l -> new Tuple2<>(l[0], Double.parseDouble(l[1])));

        JavaPairRDD<String, String> mr2 = sc.textFile("/home/bruno/Desktop/GGCD/Dados/mini/title.basics.tsv.bz2")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .filter(l -> !l.equals("\\N"))
                .mapToPair(l -> new Tuple2<>(l[0], l[2]));

        JavaPairRDD<String, Tuple2<String, Double>> mr3 = mr2.join(mr);

        JavaPairRDD<Double, Tuple2<String, String>> mr4 = mr3.mapToPair(p -> new Tuple2<>(p._2._2, new Tuple2<>(p._1, p._2._1))).sortByKey(false);

        List<Tuple2<String, Double>> ratings = mr.collect();
        List<Tuple2<String, String>> basics = mr2.collect();

        List<Tuple2<String, Tuple2<String, Double>>> result = mr3.collect();
        List<Tuple2<Double, Tuple2<String, String>>> result2 = mr4.collect();


        for(Tuple2<Double, Tuple2<String, String>> aux : result2){
            System.out.println(aux._2._1 + "\t" + aux._2._2 + "\t" + aux._1);
        }

    }
}