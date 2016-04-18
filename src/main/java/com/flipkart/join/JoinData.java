package com.flipkart.join;

import com.flipkart.constants.Enums;
import org.apache.lucene.util.OpenBitSet;

import java.util.Random;

/**
 * Created by dhritiman.das on 4/18/16.
 */
public class JoinData {

    private int [][] listingSource;  // Listing is ordinal
    private int [][] listingAttribute; // Listing is ordinal
    OpenBitSet[] shToPincodeServicability; // Source-Hash is ordinal

    private static final int LISTINGS = 10000000;  //actual per shard 10M
    private static final int MAX_SOURCES =5;  //average per listing 3-4
    private static final int MAX_ATTRIBUTES = 5;  //average per listing 5
    private static final int MAX_PINCODES = 20000; // scale to 20000
    private static final int MAX_SOURCE_HASHES = 500000; // estimated - 500K

    private static final String hashSeprator = "_".intern();
    private static final String emptyString = "".intern();

    public JoinData()
    {
        listingSource = new int[LISTINGS][MAX_SOURCES]; // Assuming a maximum of MAX_SOURCES per listing - extras will be 0 or -1
        listingAttribute = new int[LISTINGS][MAX_ATTRIBUTES]; // Assuming a maximum of MAX_ATTRIBUTES per sources
        shToPincodeServicability = new OpenBitSet[MAX_SOURCE_HASHES];
    }

    public void fillData()
    {
        //Fill Ds1
        for(int i = 0 ; i < LISTINGS ; i++)
        {
            for(int j = 0 ; j < MAX_SOURCES; j++ )
            {
                listingSource[i][j] = Enums.getSource(random(1, 20));
            }
        }

        System.out.println("Done with Listing-Source");

        //Fill Ds2
        //Consider attribute values possible from
        for(int i = 0 ; i < LISTINGS ; i++)
        {
            listingAttribute[i][0] = random(1, 10);
            listingAttribute[i][1] = random(12, 15);
            listingAttribute[i][2] = random(16, 22);
            listingAttribute[i][3] = random(23, 30);
            listingAttribute[i][4] = random(31, 34);
        }

        System.out.println("Done with Listing-Attributes");

        //Fill Ds3
        //Consider MAX_SOURCE_HASHES source-hashes and initialize each source hash with a open bit set
        //and change the
        for(int i = 0 ; i < MAX_SOURCE_HASHES; i++)
        {
            OpenBitSet bitSet = new OpenBitSet(MAX_PINCODES);
            //TODO this part is commented as it doesn't affect join time
            //else taking too much time to load this stuff
            /*
            for(int b = 0 ; b < MAX_PINCODES; b++ )
            {
                //toss a coin
                if(random(0,2) == 1)
                {
                    bitSet.set(b);
                }
            }
            */
            shToPincodeServicability[i] = bitSet;
        }

        System.out.println("Data Preparation complete");

    }

    public JoinTaskResult join(int start, int batchSize, String taskId)
    {
        long  millis = System.currentTimeMillis();
        System.out.println("Starting join for taskId - " + taskId + " at time " + millis);
        OpenBitSet[] result = new OpenBitSet[batchSize];
        int targetIndex = 0;
        for(int i = start; i < start + batchSize; i++)
        {
            OpenBitSet listingServicability = new OpenBitSet(MAX_PINCODES);

            //For each source construct the source hash
            for(int j = 0; j < MAX_SOURCES ; j++ )
            {
                //TODO ignore this source if the value if 0 or -1 or whatever we decide

                //Generate source hash
                //TODO Use shared library to compute the hash
                //TODO this string manipulation is creating a GC overhead limit exceeded error
                //Because of string manipulations. Needs to be kept in mind for the library call
                //TODO or create a separate structure with Listing to SourceHash before hand first
                // for now commenting this part as we are anyways taking a ORD

                String sourceHash = listingSource[i][j] + emptyString ;
                for(int k = 0; k < MAX_ATTRIBUTES ; k++)
                {
                    sourceHash += hashSeprator + listingAttribute[k];
                }


                //TODO lookup sourceHashToOrd map to get sourcehash ordinal
                //This source-hash will be represented by a ordinal which will be already
                //present in source-hash to ordinal map.
                //Simulate that for now with a random number lookup in the same range
                int sourceHashOrd = random(0,MAX_SOURCE_HASHES);

                //Get bitset for this sourceHash
                //TODO here we need to get the bitset from the appropriate pincode split range vertically
                //using entire range for now
                OpenBitSet bitSet = shToPincodeServicability[sourceHashOrd];
                //System.out.println("Doing a union with sourceHashOrd " + sourceHashOrd + " for listing " + i);
                listingServicability.union(bitSet);
            }
            result[targetIndex++] = listingServicability;
        }
        long  afterMillis = System.currentTimeMillis();
        System.out.println("Ending join for taskId - " + taskId + " - Elapsed = " + (afterMillis - millis)/1000);
        return new JoinTaskResult(MAX_PINCODES,start,batchSize,result);
    }

    private int random(int x, int y)
    {
        Random r = new Random();
        int low = x;
        int high = y;
        int result = r.nextInt(high-low) + low;
        return result;
    }

}
