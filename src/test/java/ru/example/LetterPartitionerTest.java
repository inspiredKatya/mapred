package ru.example;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class LetterPartitionerTest {

    @Test
    public void getPartition() throws Exception {
        WordCount.LetterPartitioner partitioner = new WordCount.LetterPartitioner();

        Assertions.assertEquals(0, partitioner.getPartition(new Text("\""), new IntWritable(1), 5));
        Assertions.assertEquals(1, partitioner.getPartition(new Text("ABC"), new IntWritable(1), 5));
        Assertions.assertEquals(4, partitioner.getPartition(new Text("ZZ9"), new IntWritable(1), 5));
        Assertions.assertEquals(1, partitioner.getPartition(new Text("абв"), new IntWritable(1), 5));
        Assertions.assertEquals(4, partitioner.getPartition(new Text("яяя"), new IntWritable(1), 5));
    }

}