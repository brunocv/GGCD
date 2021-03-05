package ggcd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Iterator;

public class Titles {

    //Mapper<numero da linha(LongWritable), cada linha string (Text), chave vai ser a palavra aka genero (Text), contagem (LongWritable)>
    public static class CountTitlesMapper extends Mapper<LongWritable, Text,Text,LongWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\t");

            for(String s : values[8].split(",")){
                context.write(new Text(s),new LongWritable(1));
            }
        }
    }

    //primeiros 2 parametros vem dos ultimos 2 parametros do Mapper que é o que o mapper produz
    //3 argumento produzir um ficheiro com os generos e contagem final logo Text (genero)
    //4 argumento também para o ficheiro mas contagem como texto por isso é que é Text também e não LongWritable
    //Reducer< Text,LongWritable,Text,Text>
    public static class CountTitlesReducer extends Reducer<Text,LongWritable, Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long total = 0;
            for(LongWritable value : values){
                total += value.get();
            }
            context.write(key,new Text(Long.toString(total)));
        }
    }

    //aline b) o que muda é garantir que combiner irá receber e retornar o mesmo tipo
    public static class CountTitlesReducerCombiner extends Reducer<Text,LongWritable, Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long total = 0;
            for(LongWritable value : values){
                total += value.get();
            }
            context.write(key,new LongWritable(total));
        }
    }

    public static void main(String args[]) throws Exception{
        //alinea 1 Map Reduce
        //alinea 2 precisam de uma classe Reducer configurada assim: job.setCombinerClass(MyCombiner.class);
        //alinea 3 medir tempo
        //alinea 4 e 5 é o bloco 05

        long startTime = System.nanoTime();

        Job job = Job.getInstance(new Configuration(),"Genres");

        //ALINEA A)
        //dizer que quero usar o jar onde está esta class
        job.setJarByClass(Titles.class);
        job.setMapperClass(CountTitlesMapper.class);
        job.setReducerClass(CountTitlesReducer.class);


        /*
        //ALINEA B)
        job.setJarByClass(Titles.class);
        job.setMapperClass(CountTitlesMapper.class);
        job.setCombinerClass(CountTitlesReducerCombiner.class);
        job.setReducerClass(CountTitlesReducerCombiner.class);
        */

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job,new Path("/home/bruno/IdeaProjects/lab1_ggcd/title.basics.tsv"));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("/home/bruno/IdeaProjects/lab1_ggcd/resultado"));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.waitForCompletion(true);

        long endTime = System.nanoTime();
        long duration = (endTime - startTime)/1000000; //miliseconds
        System.out.println("\n\nTIME: " + duration +"\n");

    }
}
