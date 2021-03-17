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

    public static class CountTitlesMapper extends Mapper<LongWritable, Text,Text,LongWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\t");

            for(String s : values[8].split(",")){
                context.write(new Text(s),new LongWritable(1));
            }
        }
    }

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

    public static void main(String args[]) throws Exception{


        //Ficheiro CLOUD com informações de criar instâncias para meter o projeto a correr
        //vídeo 10 a partir dos 9minutos mostra como meter o docker a correr pela google cloud
        //Depois é só ativar os terminais como o stor explica do docker-machine env nome... e fazer como se estivesse na máquina local

        //ERRO Failed to deploy '<unknown> Dockerfile: Dockerfile': network docker-hadoop_default not found
        //Perguntar sobre RUN OPTIONS

        //EXERCÍCIO 1,2 e 3
        //NOTA: Esta solução tem em conta eu a descomprimir o ficheiro mas não é necessário fazer o gzip desde que mudes o nome do ficheiro na linha do TextInputFormat
        //ou seja, no passo 3 era: curl --output - https://datasets.imdbws.com/title.basics.tsv.gz | hdfs dfs -put - /title.basics.tsv.gz

        //passo 1 - docker-compose up dentro de docker-hadoop
        //passo 2 - noutro terminal usar: docker exec -it namenode bash
        //passo 3 - sacar ficheiro para o docker usando (dentro do docker que é passo 2):
        //curl --output - https://datasets.imdbws.com/title.basics.tsv.gz | gzip -d | hdfs dfs -put - /title.basics.tsv  (este ultimo /title.basics.tsv é o nome que te apetecer, ou o nome que tens no programa como input vá)
        //passo 4 - vê se o ficheiro já está lá usando: hdfs dfs -ls /
        //passo 5 - no projeto do IntelliJ criar "Dockerfile" como no vídeo e meter as configurações que o stor diz no vídeo (minuto 6 até ao 12 do vídeo 8)
        //passo 6 - correr o programa pelo "Dockerfile" ou pelo run normal se já estiver configurado
        //passo 7 - hdfs dfs -cat /resultado/part-r-00000 dentro do docker para ver resultado
        //passo 8 - para voltar a correr tens de remover as diretorias no docker /tmp /app-logs e /resultado:
        //hdfs dfs -rm -r /tmp
        //hdfs dfs -rm -r /app-logs
        //hdfs dfs -rm -r /resultado


        //PS: caso queiras importar ficheiros para o docker, que estejam no teu pc, em vez de estar a sacar com o curl podes fazer (fora do docker, na pasta docker-hadoop)
        //docker run --env-file hadoop.env -v /home/bruno/IdeaProjects/lab2_ggcd/:/data --network docker-hadoop_default -it bde2020/hadoop-base hdfs dfs -put /data/title.basics.tsv /


        //EXERCÍCIO 4 e 5
        //passo 1 - procura o ficheiro title.basics.tsv no teu pc (tamanho original), se tiveres title.basics.tsv.gz extrai até teres title.basics.tsv
        //depois no terminal fazes: bzip2 title.basics.tsv para ter o ficheiro da pergunta 4
        //passo 2 - fazer dentro da pasta docker-hadoop o seguinte comando com a diretoria onde tens o title.basics.tsv.bz2, definindo o blocksize assim:
        //docker run --env-file hadoop.env -v /home/bruno/Desktop/GGCD/Dados/original/:/data --network docker-hadoop_default -it bde2020/hadoop-base hdfs dfs -D dfs.blocksize=16000000 -put /data/title.basics.tsv.bz2 /
        //passo 3 - mudar a linha do TextInputFormat para TextInputFormat.setInputPaths(job,new Path("hdfs:///title.basics.tsv.bz2")); uma vez que aceita tanto bzip2 como gz
        //não precisando de descomprimir

        //Para limpar tudo que foi feito até agora vais à pasta do dicker.hadoop e fazes: docker-compose down --volumes

        long startTime = System.nanoTime();

        Job job = Job.getInstance(new Configuration(),"Genres");

        //dizer que quero usar o jar onde está esta class
        job.setJarByClass(Titles.class);
        job.setMapperClass(CountTitlesMapper.class);
        job.setReducerClass(CountTitlesReducer.class);


        job.setInputFormatClass(TextInputFormat.class);
        //TextInputFormat.setInputPaths(job,new Path("hdfs:///title.basics.tsv"));
        TextInputFormat.setInputPaths(job,new Path("hdfs:///title.basics.tsv.gz"));
        //TextInputFormat.setInputPaths(job,new Path("hdfs:///title.basics.tsv.bz2"));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("hdfs:///resultado"));
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
