package ggcd;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Titles_4 {

    //Mapper<numero da linha(LongWritable), cada linha string (Text), chave vai ser a palavra aka genero (Text), contagem (LongWritable)>
    public static class CountTitlesMapper extends Mapper<LongWritable, Text,Text,Text>{
        HashMap<String,String> ratings = new HashMap<>();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\t");
            System.out.print(values[0]);

            if(ratings.containsKey(values[0])){
                //System.out.print("   + SIM\n"); era para debugg
                context.write(new Text(values[0]),new Text(ratings.get(values[0])));
            }
            //else era para preencher os títulos que não existem em ratings mas não é necessário
            /*else{
                //System.out.print("   + NAO\n"); era para debugg
                context.write(new Text(values[0]),new Text("0.0"));
            }*/

        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] mapsideFiles = context.getCacheFiles();
            FileSystem fs = FileSystem.get(new Configuration());

            CompressorStreamFactory csf = new CompressorStreamFactory();
            BufferedReader reader;

            for(URI u : mapsideFiles){
                //FSDataInputStream s = fs.open(new Path(u));
                try {
                    reader = new BufferedReader( new InputStreamReader(
                            csf.createCompressorInputStream(new BufferedInputStream(new FileInputStream(u.toString())))));

                    int i = 0;
                    String line = reader.readLine();
                    while ((line = reader.readLine()) != null) {

                        String[] auxLine = line.split("\t");
                        ratings.put(auxLine[0],auxLine[1]);
                        //System.out.println(auxLine[0]); era para debugg
                        i++;

                    }

                    System.out.println("Numero de linhas de title ratings: " + i);
                    reader.close();
                } catch (IOException | CompressorException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public static void main(String args[]) throws Exception{

        long startTime = System.nanoTime();

        Job job = Job.getInstance(new Configuration(),"Genres");

        job.setJarByClass(Titles_4.class);
        job.setMapperClass(CountTitlesMapper.class);

        job.setNumReduceTasks(0);
        job.addCacheFile(URI.create("/home/bruno/Desktop/GGCD/Dados/mini/title.ratings.tsv.bz2"));

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job,new Path("/home/bruno/IdeaProjects/lab1_ggcd/title.basics.tsv"));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("/home/bruno/IdeaProjects/lab1_ggcd/resultado"));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //ver estes 2 se são precisos
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.waitForCompletion(true);

        long endTime = System.nanoTime();
        long duration = (endTime - startTime)/1000000; //miliseconds
        System.out.println("\n\nTIME: " + duration +"\n");

    }
}
