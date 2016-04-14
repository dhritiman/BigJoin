package com.flipkart.constants;

/**
 * Created by dhritiman.das on 4/15/16.
 */
public class Enums {

    static int sources[] = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,1718,19,20};
    static int attributes[] = {1,2,3,4,5,6};

    public Enums()
    {
    }

    public static int getSource(int ord)
    {
        return sources[ord];
    }

    public static int getAttributes(int ord)
    {
        return attributes[ord];
    }
}
