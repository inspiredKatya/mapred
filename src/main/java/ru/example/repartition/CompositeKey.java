package ru.example.repartition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class CompositeKey implements WritableComparable<CompositeKey> {
    private String joinKey;      // customer_id or product_id
    private int sourceIndex;     // 1=Customers, 2=Orders, 3=Products
    private String recordId;     // original record ID for secondary sorting

    public CompositeKey() {}

    public CompositeKey(String joinKey, int sourceIndex, String recordId) {
        this.joinKey = joinKey;
        this.sourceIndex = sourceIndex;
        this.recordId = recordId;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(joinKey);
        out.writeInt(sourceIndex);
        out.writeUTF(recordId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        joinKey = in.readUTF();
        sourceIndex = in.readInt();
        recordId = in.readUTF();
    }

    @Override
    public int compareTo(CompositeKey other) {
        // Primary sort by joinKey
        int keyCompare = this.joinKey.compareTo(other.joinKey);
        if (keyCompare != 0) {
            return keyCompare;
        }
        // Secondary sort by sourceIndex (customers first, then orders, then products)
        return Integer.compare(this.sourceIndex, other.sourceIndex);
    }

    // Getters and setters
    public String getJoinKey() { return joinKey; }
    public int getSourceIndex() { return sourceIndex; }
    public String getRecordId() { return recordId; }

    @Override
    public String toString() {
        return joinKey + "|" + sourceIndex + "|" + recordId;
    }
}