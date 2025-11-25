package ru.example.repartition;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyGroupingComparator extends WritableComparator {

    protected CompositeKeyGroupingComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompositeKey key1 = (CompositeKey) a;
        CompositeKey key2 = (CompositeKey) b;

        // Group only by the join key, ignoring source index
        return key1.getJoinKey().compareTo(key2.getJoinKey());
    }
}