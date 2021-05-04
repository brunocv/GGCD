package ggcd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class Nr_Movies_Actor {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Number of movies per actor");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Integer> mr = sc.textFile("/home/dreamerz/Desktop/GGCD/Dados/mini/title.principals.tsv.bz2")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .filter(l -> !l.equals("\\N"))
                .filter(l -> l[3].equals("actor") || l[3].equals("actress"))
                .mapToPair(l -> new Tuple2<>(l[2], 1))
                .reduceByKey((i,j) -> i + j);

        JavaPairRDD<Integer, String> mr2 = mr.mapToPair(p -> new Tuple2<>(p._2, p._1)).sortByKey(false);

        List<Tuple2<String, Integer>> nr_movies_actor = mr.collect();
        List<Tuple2<Integer, String>> top10 = mr2.collect();

        for(Tuple2<String, Integer> aux : nr_movies_actor){
            System.out.println(aux._1 + "\t" + aux._2);
        }

        int j = 0;
        for(Tuple2<Integer, String> aux : top10){
            if(j == 10) break;
            System.out.println(aux._2 + "\t" + aux._1);
            j++;
        }
    }
}
