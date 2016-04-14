package com.flipkart.join;

import com.flipkart.constants.Enums;
import org.apache.lucene.util.OpenBitSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by dhritiman.das on 4/15/16.
 */

class AlgoImpl
{
    private int [][] listingSource;
    private int [][] listingAttribute;
    private Map<String,OpenBitSet> sourceToPins;
    OpenBitSet [] result;

    private static final int LISTINGS = 10000000;
    private static final int MAX_SOURCES = 6;
    private static final int MAX_ATTRIBUTES = 4;
    private static final int MAX_PINCODES = 20000;

    public AlgoImpl()
    {
        listingSource = new int[LISTINGS][MAX_SOURCES];
        listingAttribute = new int[LISTINGS][MAX_ATTRIBUTES];
        sourceToPins = new HashMap<String, OpenBitSet>();
        result = new OpenBitSet[LISTINGS];
    }

    public void fillData()
    {
        //Fill Ds1
        for(int i = 0 ; i < LISTINGS ; i++)
        {
            for(int j = 0 ; j < MAX_SOURCES; j++ )
            {
                listingSource[i][j] = Enums.getSource(random(0,20));
            }
        }

        //Fill Ds2
        //Consider attribute values possible from
        for(int i = 0 ; i < LISTINGS ; i++)
        {
            listingAttribute[i][0] = random(1, 10);
            listingAttribute[i][1] = random(12, 15);
            listingAttribute[i][2] = random(16, 22);
            listingAttribute[i][3] = random(23, 30);
        }

        //Fill Ds3
        //Consider 20000 sources and the above ranges for possible attribute values
        for(int i = 0 ; i < 20000; i++)
        {
            for(int j = 1 ; j < 10; j++)
            {
                for( int k = 12; k < 15; k++)
                {
                    for( int l=16 ; l < 22 ; l++)
                    {
                        for(int m = 23 ; m < 30 ; m++)
                        {
                            String hash = i+ "_" + j + "_" + k + "_" + l + "_" + m;
                            OpenBitSet bitSet = new OpenBitSet();
                            bitSet.ensureCapacity(MAX_PINCODES);
                            for(int b = 0 ; b < MAX_PINCODES; b++ )
                            {
                                //toss a coin
                                if(random(0,2) == 1)
                                {
                                    bitSet.set(b);
                                }
                            }
                            sourceToPins.put(hash,bitSet);
                        }
                    }
                }
            }
        }

    }

    public void join(int start, int end)
    {
        for(int i = start; i < end; i++)
        {
            OpenBitSet listingServicability = new OpenBitSet();
            listingServicability.ensureCapacity(MAX_PINCODES);

            //For each source construct the source hash
            for(int j = 0; j < MAX_SOURCES ; j++ )
            {
                //Generate source hash
                String sourceHash = listingSource[i][j] + "";
                for(int k = 0; k < MAX_ATTRIBUTES ; k++)
                {
                    sourceHash += "_" + listingAttribute[k];
                }
                //Get bitset for this sourceHash
                System.out.println("Getting bitset for hash " + sourceHash);
                OpenBitSet bitSet = sourceToPins.get(sourceHash);
                listingServicability.union(bitSet);
            }
            result[i] = listingServicability;
        }
        System.out.println("Done");
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

public class NaiveJoin {

    public static void main(String [] args)
    {
        AlgoImpl impl = new AlgoImpl();
        impl.join(0,1000000);
    }
}
