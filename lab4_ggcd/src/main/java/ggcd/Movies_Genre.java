package ggcd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Movies_Genre {

    ///home/bruno/Desktop/GGCD/Dados/mini

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("g0spark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Integer> mr = sc.textFile("/home/bruno/Desktop/GGCD/Dados/mini/title.basics.tsv.bz2")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .map(l -> l[8])
                .filter(l -> !l.equals("\\N"))
                .flatMap(l -> Arrays.asList(l.split(",")).iterator())
                .mapToPair(l -> new Tuple2<>(l, 1))
                .reduceByKey((i, j) -> i + j);


        List<Tuple2<String, Integer>> genres = mr.collect();

        for(Tuple2<String, Integer> aux : genres){
            System.out.println(aux._1 + "\t" + aux._2);
        }
    }
}
