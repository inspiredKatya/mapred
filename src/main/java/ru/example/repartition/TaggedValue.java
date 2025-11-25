package ru.example.repartition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class TaggedValue implements Writable {
    private int sourceIndex;
    private String data;

    public TaggedValue() {}

    public TaggedValue(int sourceIndex, String data) {
        this.sourceIndex = sourceIndex;
        this.data = data;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(sourceIndex);
        out.writeUTF(data);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sourceIndex = in.readInt();
        data = in.readUTF();
    }

    // Getters and setters
    public int getSourceIndex() { return sourceIndex; }
    public String getData() { return data; }

    @Override
    public String toString() {
        return sourceIndex + ":" + data;
    }
}