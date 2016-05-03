package com.flipkart.mapreduce;

/**
 * Created by dhritiman.das on 4/25/16.
 */
public interface ReaderFactory<K> {

    PartitionAwareIterator<K> getReaderForPartition(int partitionId);
}
