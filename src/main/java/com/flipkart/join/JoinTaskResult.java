package com.flipkart.join;

import org.apache.lucene.util.OpenBitSet;

/**
 * Created by dhritiman.das on 4/18/16.
 */
public class JoinTaskResult {

    public OpenBitSet[] resultBitset;
    public int size ;
    public long start;
    public long numListings;

    public JoinTaskResult(int bitsetSize, long start, long numListings, OpenBitSet[] result)
    {
        this.size = bitsetSize;
        this.start = start;
        this.numListings = numListings;
        resultBitset = result;
    }
}
