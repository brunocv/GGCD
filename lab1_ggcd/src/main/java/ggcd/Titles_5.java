package ggcd;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Titles_5{

    //Não era necessário usar 2 Maps porque 2 Maps é para quando se tem 2 ficheiros grandes, mas assim já fica para aprender em vez de se fazer como no exe 4
    //este é para o title.basics e fica com (numero do filme, 1)
    public static class LeftMapper extends Mapper<LongWritable, Text,Text,Text>{
        //este é para o title.basics e fica com (numero do filme, 1)
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\t");

            if(!values[0].equals("tconst")) context.write(new Text(values[0]),new Text("null"));

        }
    }

    //este é para o ficheiro ratings e fica com (numero do filme , rating)
    public static class RightMapper extends Mapper<LongWritable, Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\t");

            if((!values[0].equals("tconst")) && (Double.parseDouble(values[1]))>=9) context.write(new Text(values[0]),new Text(values[1]));

        }
    }

    public static class JoinReducer extends Reducer<Text,Text, Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for(Text value : values){
                if( !value.toString().equals("null") && Double.parseDouble(value.toString()) > 0) context.write(key,new Text(value));
            }

        }
    }

    public static class SortMapper extends Mapper<LongWritable, Text, DoubleWritable,Text>{
        //este é para o title.basics e fica com (numero do filme, 1)
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\t");

            context.write(new DoubleWritable(Double.parseDouble(values[1])),new Text(values[0]));
            System.out.println("\n\n ENTREI \n");
        }
    }

    public static class SortReducer extends Reducer<DoubleWritable,Text, Text,Text>{
        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for(Text value : values){
                context.write(new Text(value),new Text(key.toString()));
            }

        }
    }
    //  Resultado = /home/bruno/IdeaProjects/lab1_ggcd/resultado/part-r-00000

    public static void main(String args[]) throws Exception{

        long startTime = System.nanoTime();

        Job job = Job.getInstance(new Configuration(),"Ratings");

        job.setJarByClass(Titles_5.class);


        MultipleInputs.addInputPath(job,new Path("/home/bruno/IdeaProjects/lab1_ggcd/title.basics.tsv"),
                TextInputFormat.class, LeftMapper.class);

        MultipleInputs.addInputPath(job,new Path("/home/bruno/IdeaProjects/lab1_ggcd/title.ratings.tsv"),
                TextInputFormat.class, RightMapper.class);
        job.setReducerClass(JoinReducer.class);


        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("/home/bruno/IdeaProjects/lab1_ggcd/resultado"));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //ver estes 2 se são precisos
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.waitForCompletion(true);

        // SORT JOB

        Job job2 = Job.getInstance(new Configuration(),"Sort");

        job2.setJarByClass(Titles_5.class);
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);

        job2.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job2,new Path("/home/bruno/IdeaProjects/lab1_ggcd/resultado/part-r-00000"));

        job2.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job2,new Path("/home/bruno/IdeaProjects/lab1_ggcd/resultadoSort"));
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setMapOutputKeyClass(DoubleWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.waitForCompletion(true);


        long endTime = System.nanoTime();
        long duration = (endTime - startTime)/1000000; //miliseconds
        System.out.println("\n\nTIME: " + duration +"\n");

    }
}
