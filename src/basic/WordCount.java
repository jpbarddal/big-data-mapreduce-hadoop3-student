package basic;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;


public class WordCount {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "wordcount-professor");

        // registro das classes
        j.setJarByClass(WordCount.class);
        j.setMapperClass(MapForWordCount.class);
        j.setReducerClass(ReduceForWordCount.class);

        // definicao dos tipos de saida
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * Classe de MAP
     * Formato geral: Mapper<Tipo da chave de entrada,
     *                       Tipo da entrada,
     *                       Tipo da chave de saida,
     *                       Tipo da saida>
     */
    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // Carregamento do bloco e conversao dele para string
            String line = value.toString();

            // fazendo o split para obter as palavras de forma isolada
            String[] words = line.split(" ");

            // para cada palavra
            for (String word : words) {
                /**
                 * Montando a saida no formato <chave: palavra, valor: 1>
                 */

                // Limpando cada chave
                String formattedOutput = word.toUpperCase()
                        .trim()
                        .replace(",", "")
                        .replace(".", "");
                // Chave
                Text outputKey = new Text(formattedOutput);
                // Valor
                IntWritable outputValue = new IntWritable(1);
                con.write(outputKey, outputValue);
            }
        }
    }


    /**
     * Classe de Reduce
     * Formato geral: Reducer<Tipo da chave de Entrada,
     *                        Tipo do Valor de Entrada,
     *                        Tipo da chave de Saida,
     *                        Tipo do Valor de Saida>
     *
     *
     * Importante: note que o tipo do valor de entrada n eh uma lista!
     */
    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        // Funcao de reduce
        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            // Objetivo: somar as ocorrencias de uma palavra
            // Note que o reduce vai ser chamado uma vez por chave,
            // entao nao precisamos filtrar por palavra
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            // faz a saida no formato <palavra, somatorio de ocorrencias>
            con.write(key, new IntWritable(sum));
        }
    }

}
