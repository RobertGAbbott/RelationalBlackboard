/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.examples.util;

import java.util.ArrayList;

/**
 *
 * @author rgabbot
 */
public class DynamicTimeWarping {

    static void printArray(float[][] a) {
        for(float[] a0 : a) {
            for(float x : a0) {
                System.err.print(x == Float.MAX_VALUE ? "INF" : Float.toString(x));
                System.err.print(" ");
            }
            System.err.println("");
        }
    }



    enum Step { Repeat_Neither, Repeat_B, Repeat_A };

    /**
     *
     * The alignment ai created by inserting repeats into a and b.
     *
     * @param a
     * @param b
     * @param ai
     * @param bi
     * @return
     */

    public static float DTWDistance(float[] a, float[] b, ArrayList<Integer> ai, ArrayList<Integer> bi) {
        // pathCost[i][j] is the cost of aligning a[i] with b[j], including the lowest-cost path to that point.
        float[][] pathCost = new float[a.length+1][];
        for(int i=0; i <= a.length; ++i)
            pathCost[i] = new float[b.length+1];
        Step steps[][] = new Step[a.length][];
        for(int i = 0; i < a.length; ++i)
            steps[i] = new Step[b.length];

        // first row & col are infinity
        for(int i=1; i <= a.length; ++i)
            pathCost[i][0] = Float.MAX_VALUE;
        for(int j=1; j <= b.length; ++j)
            pathCost[0][j] = Float.MAX_VALUE;
        pathCost[0][0] = 0.0f;

        for(int i = 1; i <= a.length; ++i)
            for(int j = 1; j <= b.length; ++j) {

                // tentatively assume match
                pathCost[i][j] = pathCost[i-1][j-1];
                steps[i-1][j-1] = Step.Repeat_Neither;

                // consider repeating the current symbol in b
                // by pairing the same b with the previous a
                if(pathCost[i][j] > pathCost[i-1][j]) {
                    pathCost[i][j] = pathCost[i-1][j];
                    steps[i-1][j-1] = Step.Repeat_B;
                }

                // consider repeating the current symbol in b
                if(pathCost[i][j] > pathCost[i][j-1]) {
                     pathCost[i][j] = pathCost[i][j-1];
                     steps[i-1][j-1] = Step.Repeat_A;
                }

                pathCost[i][j] += Math.abs(a[i-1]-b[j-1]);
            }

            printArray(pathCost);
            System.err.println("");

            int i = a.length-1;
            int j = b.length-1;

            while(i>=0 || j>=0) {

                System.err.println(i + ", " + j + "(" + steps[i][j] + ")");

                ai.add(0, i);
                bi.add(0, j);
                switch(steps[i][j]) {
                    case Repeat_Neither: --i; --j; break;
                    case Repeat_B: --i; break;
                    case Repeat_A: --j; break;
                }
            }


//        int[] map = new int[a.length];
//        buildMap(steps, a.length-1, b.length-1, map, a.length-1);
//
//        System.err.print("Map: ");
//        for(int i = 0; i < map.length; ++i)
//            System.err.print(map[i]+ " ");
//        System.err.println("");

//        return map;

        return pathCost[a.length][b.length];
    }

//    static void buildMap(Step[][] steps, int iStep, int jStep, int[] map, int iMap) {
//
//        map[iMap] = jStep;
//
//        if(iMap==0)
//            return;
//
//        switch(steps[iStep][jStep]) {
//            case Repeat_Neither:
//                buildMap(steps, iStep-1, jStep-1, map, iMap-1);
//                break;
//            case Repeat_B:
//                buildMap(steps, iStep-1, jStep, map, iMap-1);
//                break;
//            case Repeat_A:
//                break;
//        }
//    }

    static void test(float[] a, float[] b) {
        ArrayList<Integer> is = new ArrayList<Integer>();
        ArrayList<Integer> it = new ArrayList<Integer>();
        DTWDistance(a,b, is, it);

        System.err.println("Unaligned:");

        for(int i = 0; i < a.length; ++i)
            System.err.print(a[i]+ " ");
        System.err.println("");

        for(int i = 0; i < b.length; ++i)
           System.err.print(b[i]+ " ");
        System.err.println("");


        System.err.println("Aligned:");

        for(int i = 0; i < is.size(); ++i)
            System.err.print(a[is.get(i)]+ " ");
        System.err.println("");

        for(int i = 0; i < it.size(); ++i)
            System.err.print(b[it.get(i)]+ " ");
        System.err.println("");


        System.err.println("");


}

    // each dtw[i][j] is the lowest sub-cost of any warping that
    // pairs s[i] and t[j]

    public static void main(String[] args) {
        test(new float[] { 1, 0, 1, 0, 1, 0,  1 },
             new float[] { 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0.1f, 0, 1, 1 } );

        

    }
}
