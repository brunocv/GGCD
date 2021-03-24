package ggcd;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.awt.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class ToParquet {

    public static Schema getSchema() throws IOException{
        InputStream is = new FileInputStream("schema.parquet");
        String ps = new String(is.readAllBytes());
        MessageType mt = MessageTypeParser.parseMessageType(ps);
        return new AvroSchemaConverter().convert(mt);
    }

    public static class ToParquetMapper extends Mapper<LongWritable, Text, Void, GenericRecord> {

        private Schema schema;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            schema = getSchema();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //passar a primeira linha
            if (key.get() == 0) return;

            GenericRecord record = new GenericData.Record(schema);

            String[] values = value.toString().split("\t");

            //estes nomes de key tem de ser o que está em schema.parquet
            record.put("tconst",values[0]);


            List<String> genres = new ArrayList<>();

            for(String s : values[8].split(",")){
                if(!s.equals("\\N"))genres.add(s);
            }
            //estes nomes de key tem de ser o que está em schema.parquet
            record.put("genres",genres);

            context.write(null,record);
        }
    }

    public static void main(String[] args) throws Exception{

        Job job = Job.getInstance(new Configuration(), "ToParquet");
        job.setJarByClass(ToParquet.class);
        job.setMapperClass(ToParquetMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(GenericRecord.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job,new Path("/home/bruno/Desktop/GGCD/Dados/original/title.basics.tsv.gz"));

        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setSchema(job, getSchema());
        FileOutputFormat.setOutputPath(job,new Path("resultado"));

        job.waitForCompletion(true);
    }
}
