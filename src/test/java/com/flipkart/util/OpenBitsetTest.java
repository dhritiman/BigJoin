package com.flipkart.util;

import org.apache.lucene.util.OpenBitSet;

import java.util.Random;

/**
 * Created by dhritiman.das on 4/17/16.
 */
public class OpenBitsetTest {

    public static void main(String [] args)
    {
        int size = 8;
        OpenBitSet b1 = new OpenBitSet(size);
        randomizeBitSet(b1,size);
        printOpenBitSet(b1, size);

        OpenBitSet b2 = new OpenBitSet(size);
        randomizeBitSet(b2,size);
        printOpenBitSet(b2, size);

        b1.union(b2);
        printOpenBitSet(b1, size);

    } 
    private static void randomizeBitSet(OpenBitSet bitSet, long size)
    {
        for(int b = 0 ; b < size; b++ )
        {
            //toss a coin
            if(random(0,2) == 1)
            {
                bitSet.set(b);
            }
        }
    }

    private static void printOpenBitSet(OpenBitSet bitSet, long size) {
        System.out.println("Printing Bitset : Size " + size);
        for(int i = 0 ; i < size; i++)
        {
            System.out.print(bitSet.get(i) ? 1 : 0);
        }
        System.out.println();
    }

    private static int random(int x, int y)
    {
        Random r = new Random();
        int low = x;
        int high = y;
        int result = r.nextInt(high-low) + low;
        return result;
    }
}
