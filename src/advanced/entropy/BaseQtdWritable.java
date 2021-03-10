package advanced.entropy;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class BaseQtdWritable implements WritableComparable<BaseQtdWritable> {

    private String base;
    private long qtd;

    public BaseQtdWritable() {
    }

    public BaseQtdWritable(String base, long qtd) {
        this.base = base;
        this.qtd = qtd;
    }

    public String getBase() {
        return base;
    }

    public void setBase(String base) {
        this.base = base;
    }

    public long getQtd() {
        return qtd;
    }

    public void setQtd(long qtd) {
        this.qtd = qtd;
    }

    @Override
    public String toString() {
        return "BaseQtdWritable{" +
                "base='" + base + '\'' +
                ", qtd=" + qtd +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(base);
        dataOutput.writeUTF(String.valueOf(qtd));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        base = dataInput.readUTF();
        qtd = Long.parseLong(dataInput.readUTF());
    }

    @Override
    public int compareTo(BaseQtdWritable o) {
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
        BaseQtdWritable that = (BaseQtdWritable) o;
        return qtd == that.qtd && Objects.equals(base, that.base);
    }

    @Override
    public int hashCode() {
        return Objects.hash(base, qtd);
    }
}