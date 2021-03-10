package advanced.entropy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EntropyFASTA {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        Path intermediate = new Path("./output/intermediate.tmp");

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "entropy-fasta-pt1");

        // registro das classes
        j.setJarByClass(EntropyFASTA.class);
        j.setMapperClass(MapEtapaA.class);
        j.setReducerClass(ReduceEtapaA.class);
        j.setCombinerClass(ReduceEtapaA.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(LongWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(LongWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, intermediate);

        // lanca o job e aguarda sua execucao
        if (!j.waitForCompletion(true)){
            System.out.println("Houve um erro na etapa 1.");
            return;
        }

        // Concatenando o job2 ao job 1

        Job j2 = new Job(c, "entropy-fasta-pt2");

        // registro de classes
        j2.setJarByClass(EntropyFASTA.class);
        j2.setMapperClass(MapEtapaB.class);
        j2.setReducerClass(ReduceEtapaB.class);
        // TODO: add a combiner here
        // j2.setCombinerClass();


        // definicao dos tipos de saida
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(BaseQtdWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(DoubleWritable.class);

        // cadastro dos arquivos de entrada e saida

        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);

        // lanca o job e aguarda sua execucao
        if(!j2.waitForCompletion(true)) {
            System.out.println("Houve um erro na etapa 2");
        }
    }


    public static class MapEtapaA extends Mapper<LongWritable, Text, Text, LongWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // obtem conteudo da linha
            String line = value.toString();

            // ignora header
            if(line.startsWith(">")) return;

            // bases ctgan
            String[] bases = line.split("");

            for(String b : bases) {
                // emitir chave-valor (base,1)
                con.write(new Text(b), new LongWritable(1));

                // emitir chave-valor (total, 1)
                con.write(new Text("total"), new LongWritable(1));
            }
        }
    }

    public static class ReduceEtapaA extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context con)
                throws IOException, InterruptedException {
            // soma os valores provenientes da lista
            long soma = 0;
            for (LongWritable v : values){
                soma += v.get();
            }
            // emite chave-valor (base, qtd total da base) ou (total, qtd total do dataset)
            con.write(key, new LongWritable(soma));
        }
    }


    public static class MapEtapaB extends Mapper<LongWritable, Text, Text, BaseQtdWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // emitir chave comum e (base, qtd)
            String[] vals = value.toString().split("\t");
            con.write(new Text("agg"),
                    new BaseQtdWritable(vals[0], Long.parseLong(vals[1])));

//            con.write(new Text("agg"), new BaseQtdWritable(key.toString(), value.get()));

        }
    }

    public static class ReduceEtapaB extends Reducer<Text, BaseQtdWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<BaseQtdWritable> values, Context con)
                throws IOException, InterruptedException {
            long totalBases = 0;
            for(BaseQtdWritable v : values){
                if (v.getBase().equals("total")){
                    totalBases = v.getQtd();
                    break;
                }
            }

            for(BaseQtdWritable v : values){
                if(!v.getBase().equals("total")){
                    double p = v.getQtd() / (double) totalBases;
                    double entropy = -p * Math.log10(p) / Math.log10(2.0);
                    con.write(new Text(v.getBase()), new DoubleWritable(entropy));
                }
            }
        }
    }

}
