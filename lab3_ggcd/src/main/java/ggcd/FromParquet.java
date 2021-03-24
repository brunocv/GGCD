package ggcd;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class FromParquet {

    public static Schema getSchema() throws IOException{
        InputStream is = new FileInputStream("schema.parquet2");
        String ps = new String(is.readAllBytes());
        MessageType mt = MessageTypeParser.parseMessageType(ps);
        return new AvroSchemaConverter().convert(mt);
    }

    public static class FromParquetMapper extends Mapper<Void, GenericRecord, Text, LongWritable>{

        @Override
        protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {

            List<String> values = new ArrayList<>();
            values = (List<String>) value.get("genres");

            for(String s : values){
                context.write(new Text(s),new LongWritable(1));
            }
        }
    }

    public static class FromParquetReducer extends Reducer<Text,LongWritable, Text,LongWritable> {
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

        long startTime = System.nanoTime();

        Job job = Job.getInstance(new Configuration(),"FromParquet");

        job.setJarByClass(FromParquet.class);
        job.setMapperClass(FromParquetMapper.class);
        job.setReducerClass(FromParquetReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job,new Path("resultado"));
        //Configuração para ignorar o resto das colunas uma vez que só preciso da do tipo de filme (alinea 4)
        //alinea 4 (3 linhas)
        Schema schema;
        schema = getSchema();
        AvroParquetInputFormat.setRequestedProjection(job, schema);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("resultado2"));

        job.waitForCompletion(true);

        long endTime = System.nanoTime();
        long duration = (endTime - startTime)/1000000; //miliseconds
        System.out.println("\n\nTIME: " + duration +"\n");

    }


}
