package ggcd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Array;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class Top3_Movies_Actor {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Number of movies per actor");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, String> mr = sc.textFile("/home/bruno/Desktop/GGCD/Dados/mini/title.principals.tsv.bz2")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .filter(l -> !l.equals("\\N"))
                .filter(l -> l[3].equals("actor") || l[3].equals("actress"))
                .mapToPair(l -> new Tuple2<>(l[0], l[2]));

        JavaPairRDD<String, String> mr2 = sc.textFile("/home/bruno/Desktop/GGCD/Dados/mini/title.ratings.tsv.bz2")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .filter(l -> !l.equals("\\N"))
                .mapToPair(l -> new Tuple2<>(l[0], l[1]));

        JavaPairRDD<String, Iterable<Tuple2<String, String>>> mr3 = mr.join(mr2)
                .mapToPair(l -> new Tuple2<>(l._2._1, new Tuple2<>(l._1, l._2._2))).sortByKey().groupByKey();

        List<Tuple2<String, String>> actors = mr.collect();
        List<Tuple2<String, String>> ratings = mr2.collect();

        //ator , filme , rating
        List<Tuple2<String,  Iterable<Tuple2<String, String>>>> result = mr3.collect();

        int j = 1;
        for(Tuple2<String,  Iterable<Tuple2<String, String>>> aux : result){
            ArrayList<String> sorted = new ArrayList<>();
            sorted = getSortedArrayList(aux._2);
            j = 1;
            System.out.println("Actor/Actress: " + aux._1);
            for(String aux2 : sorted){
                if(j==4) break;
                System.out.println("Top " + j +": " + aux2);
                j++;
            }
        }
    }

    public static ArrayList<String> getSortedArrayList(Iterable<Tuple2<String, String>> it){
        ArrayList<String> result = new ArrayList<>();

        for(Tuple2<String, String> aux : it){
            result.add(aux._1 + "\t" + aux._2);
        }

        Collections.sort(result, new Comparator<String>() {
            public int compare(String s1, String s2) {
                String[] s1_aux = s1.split("\t");
                String[] s2_aux = s2.split("\t");
                Double a = Double.parseDouble(s1_aux[1]);
                Double b = Double.parseDouble(s2_aux[1]);
                return b.compareTo(a);
            }
        });

        return result;

    }




}

