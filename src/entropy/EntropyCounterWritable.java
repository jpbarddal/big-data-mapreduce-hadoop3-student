package entropy;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.awt.image.DataBufferInt;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EntropyCounterWritable implements WritableComparable<EntropyCounterWritable> {

    private int qtd;
    private int totalChunk;
    private String content;

    public EntropyCounterWritable() {
    }

    public EntropyCounterWritable(int qtd, String content) {
        this.qtd = qtd;
        this.totalChunk = 0;
        this.content = content;
    }

    public EntropyCounterWritable(int qtd, int totalChunk,
                                   String content) {
        this.qtd = qtd;
        this.totalChunk = totalChunk;
        this.content = content;
    }

    // Example: EntropyCounterWritable{qtd=70, totalChunk=248710, content='N'}
    public EntropyCounterWritable(String obj) {
        Pattern p = Pattern.compile("\\d+");
        this.qtd = Integer.parseInt(obj.split("qtd=")[1].split(",")[0]);
        this.totalChunk = Integer.parseInt(obj.split("totalChunk=")[1].split(",")[0]);
        this.content = obj.split("content='")[1].replace("'}", "");
    }

    public EntropyCounterWritable(EntropyCounterWritable obj) {
        this.qtd = obj.qtd;
        this.totalChunk = obj.totalChunk;
        this.content = obj.content;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(String.valueOf(qtd));
        dataOutput.writeUTF(String.valueOf(totalChunk));
        dataOutput.writeUTF(content);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        qtd = Integer.parseInt(dataInput.readUTF());
        totalChunk = Integer.parseInt(dataInput.readUTF());
        content = dataInput.readUTF();
    }

    public int getQtd() {
        return qtd;
    }

    public void setQtd(int qtd) {
        this.qtd = qtd;
    }

    public int getTotalChunk() {
        return totalChunk;
    }

    public void setTotalChunk(int totalChunk) {
        this.totalChunk = totalChunk;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public String toString() {
        return "EntropyCounterWritable{" +
                "qtd=" + qtd +
                ", totalChunk=" + totalChunk +
                ", content='" + content + '\'' +
                '}';
    }

    @Override
    public int compareTo(EntropyCounterWritable o) {
        if(this.hashCode() < o.hashCode()){
            return -1;
        }else if(this.hashCode() > o.hashCode()){
            return +1;
        }else{
            return 0;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EntropyCounterWritable that = (EntropyCounterWritable) o;
        return qtd == that.qtd && totalChunk == that.totalChunk && Objects.equals(content, that.content);
    }

    @Override
    public int hashCode() {
        return Objects.hash(qtd, totalChunk, content);
    }
}
