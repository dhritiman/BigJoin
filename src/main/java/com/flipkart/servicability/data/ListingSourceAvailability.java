package com.flipkart.servicability.data;

import java.util.*;

/**
 * Created by dhritiman.das on 4/14/16.
 */
public class ListingSourceAvailability implements Iterable<Map.Entry<Integer,HashSet<Integer>>>{

    // ListingId mapped to a Integer Ord, keep separate maps Listing->Integer
    // and Integer->Listing
    // Source Id is mapped to a integer
    private Map<Integer, HashSet<Integer>> sourceAvailability;

    public void addAvailableSourceForListing(Integer listing, Integer source)
    {
        if(sourceAvailability.containsKey(listing))
        {
            HashSet<Integer> availableSources = sourceAvailability.get(listing);
            availableSources.add(source);
        }
        else
        {
            HashSet<Integer> sources =  new HashSet<Integer>();
            sources.add(source);
            sourceAvailability.put(listing, sources);
        }
    }

    public void removeAvailableSourceForListing(Integer listing, Integer source)
    {
        if(sourceAvailability.containsKey(listing))
        {
            HashSet<Integer> availableSources = sourceAvailability.get(listing);
            availableSources.remove(source);
        }

    }

    public HashSet<Integer> getAvailableSources(Integer listing)
    {
        return sourceAvailability.get( listing );
    }

    /*
    public Map<Integer, HashSet<Integer>> getAvailableSources()
    {
        return sourceAvailability;
    }*/

    public ListingSourceAvailability()
    {
        sourceAvailability = new HashMap<Integer, HashSet<Integer>>();
    }

    @Override
    public Iterator<Map.Entry<Integer, HashSet<Integer>>> iterator() {
        return new ListingSourceAvailabilityIterator(sourceAvailability.entrySet().iterator());
    }


    private static class ListingSourceAvailabilityIterator implements Iterator<Map.Entry<Integer,HashSet<Integer>>>
    {
        private Iterator internalIterator;

        public ListingSourceAvailabilityIterator(Iterator internalIterator)
        {
            this.internalIterator = internalIterator;
        }

        @Override
        public boolean hasNext() {
            return internalIterator.hasNext();
        }

        @Override
        public Map.Entry<Integer, HashSet<Integer>> next() {
            if(!this.hasNext())
            {
                throw new NoSuchElementException();
            }
            Map.Entry<Integer,HashSet<Integer>> retVal = (Map.Entry<Integer,HashSet<Integer>>)internalIterator.next();
            return retVal;
        }

        @Override
        public void remove() {
            //Does nothing for now
        }
    }

}

