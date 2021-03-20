package basic

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.log4j.BasicConfigurator
import java.io.File
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    System.setProperty("hadoop.home.dir", "/opt/hadoop-3.2.1")

    BasicConfigurator.configure()

    val c = Configuration()
    val files = GenericOptionsParser(c, args).remainingArgs

    val input = Path(files[0])
    val output = Path(files[1])

    // Deleta o folder de resultado antes de gerar um novo
    val outFile = File(output.toString())
    outFile.deleteRecursively()

    val job = Job(c, "wordcount").apply { // uso do apply é opcional

        mapperClass = MapForWordCount::class.java
        reducerClass = ReduceForWordCount::class.java

        // definicao dos tipos de saida
        mapOutputKeyClass = Text::class.java // chave de saida do map
        mapOutputValueClass = IntWritable::class.java // valor de saida do map
        outputKeyClass = Text::class.java // chave de saida do reduce
        outputValueClass = IntWritable::class.java // valor de saida do reduce
    }

    FileInputFormat.addInputPath(job, input)
    FileOutputFormat.setOutputPath(job, output)
    exitProcess(if (job.waitForCompletion(true)) 0 else 1)
}

class MapForWordCount : Mapper<LongWritable, Text, Text, IntWritable>() {

    override fun map(key: LongWritable, value: Text, con: Context) {
        val linha = value.toString()
        val palavras: List<String> = linha.split(" ")
        palavras.forEach { p ->
            val chaveSaida = Text(p.toLowerCase())
            val valorSaida = IntWritable(1)
            con.write(chaveSaida, valorSaida)
        }
    }
}

class ReduceForWordCount : Reducer<Text, IntWritable, Text, IntWritable>() {

    override fun reduce(key: Text, values: Iterable<IntWritable>, con: Context) {
        val soma = values.sumBy { it.get() }
        con.write(key, IntWritable(soma))
    }
}
