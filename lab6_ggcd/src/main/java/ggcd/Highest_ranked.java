package ggcd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Serializable;
import scala.Tuple2;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.List;

class ComparadorTuplos implements Comparator<Tuple2<String, Double>>, Serializable {
    final static ComparadorTuplos INSTANCE = new ComparadorTuplos();
    public int compare(Tuple2<String, Double> t1, Tuple2<String, Double> t2) {
        return -t1._2.compareTo(t2._2);
    }
}

public class Highest_ranked {
    public static void main(String[] args) throws Exception{
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("savefile");

        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(60));

        int windowB = 10;
        int slideB = 60;


        //ver bug de dizer que x filmes tem 10.0 quando nao tem
        sc.socketTextStream("localhost", 12345)
                .window(Durations.minutes(windowB),Durations.seconds(slideB))
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .mapToPair(l -> new Tuple2<>(l[0], new Tuple2<>(Double.parseDouble(l[1]),1)))
                .reduceByKeyAndWindow((Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>>) (i1,i2) ->
                        new Tuple2<>(i1._1 + i2._1, i1._2 + i2._2), Durations.minutes(windowB), Durations.seconds(slideB))
                .mapToPair(v -> new Tuple2<>(v._1, v._2._1 / v._2._2))
                .foreachRDD(rdd -> {
                    List<Tuple2<String, Double>> res = rdd
                            .takeOrdered(3, new ComparadorTuplos());

                    System.out.println( "TOP 3: " + res.toString());
                });

        sc.start();
        sc.awaitTermination();
    }
}
