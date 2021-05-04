package ggcd;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class SaveToFile {
    public static void main(String[] args) throws Exception{
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("savefile");

        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(60));

        sc.socketTextStream("localhost", 12345)
                .foreachRDD( rdd -> {
                            String folder = LocalDateTime.now().truncatedTo(ChronoUnit.MINUTES)
                                    .toString().replace(":", "-");
                            rdd.saveAsTextFile("/home/dreamerz/IdeaProjects/lab6_ggcd/Resultado/" + folder);
                        });


        sc.start();
        sc.awaitTermination();
    }
}
