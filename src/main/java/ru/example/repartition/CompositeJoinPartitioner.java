package ru.example.repartition;

import org.apache.hadoop.mapreduce.Partitioner;

public class CompositeJoinPartitioner extends Partitioner<CompositeKey, TaggedValue> {

    @Override
    public int getPartition(CompositeKey key, TaggedValue value, int numPartitions) {
        // Partition only by the join key to ensure all records with same join key
        // go to the same reducer, regardless of source
        return Math.abs(key.getJoinKey().hashCode()) % numPartitions;
    }
}
