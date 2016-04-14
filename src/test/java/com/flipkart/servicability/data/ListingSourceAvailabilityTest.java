package com.flipkart.servicability.data;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by dhritiman.das on 4/14/16.
 */
public class ListingSourceAvailabilityTest {

    public static void main(String [] args)
    {
        ListingSourceAvailability lsa = new ListingSourceAvailability();

        lsa.addAvailableSourceForListing(1,2);
        lsa.addAvailableSourceForListing(1,3);
        lsa.addAvailableSourceForListing(2,1);
        lsa.addAvailableSourceForListing(1,4);
        lsa.addAvailableSourceForListing(3,3);
        lsa.addAvailableSourceForListing(4,1);
        lsa.addAvailableSourceForListing(4,2);

        Iterator it = lsa.iterator();
        while(it.hasNext())
        {
            Map.Entry<Integer,HashSet<Integer>> entry = (Map.Entry<Integer,HashSet<Integer>>)it.next();
            System.out.print("Listing " + entry.getKey()+ "-->");
            for(Integer source : entry.getValue())
            {
                System.out.print(source + " ");
            }
            System.out.println(" ");
        }
    }
}
