package ggcd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class Collaborators {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Number of movies per actor");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //cada ator tera os seus filmes
        JavaPairRDD<String, String> mr = sc.textFile("/home/dreamerz/Desktop/GGCD/Dados/mini/title.principals.tsv.bz2")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .filter(l -> !l.equals("\\N"))
                .filter(l -> l[3].equals("actor") || l[3].equals("actress"))
                .mapToPair(l -> new Tuple2<>(l[0], l[2]))
                .cache();

        mr.join(mr)
                .filter(p -> !p._2._1.equals(p._2._2))
                .mapToPair(p -> p._2)
                .groupByKey()
                .saveAsTextFile("resultado/collaborators");

        //filme , iterable(atores)               f1, a2|a3|a4   -  f2, a1|a2|a4   -> final a1 (a2, a4) || a2 (a1, a3, a4)...
        List<Tuple2<String, String>> actors = mr.collect();


    }
}
