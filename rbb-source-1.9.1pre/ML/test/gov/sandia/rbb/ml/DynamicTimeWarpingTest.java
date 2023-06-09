/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml;

import java.util.ArrayList;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author rgabbot
 */
public class DynamicTimeWarpingTest {

    public static class DrawData {

        DrawData(int sampleRate) {
            reset();
            this.sampleRate = sampleRate;
        }

        ArrayList<double[]> data;

        /**
         * Steps per unit distance used in subsequent calls.
         */
        int sampleRate;

        void reset() {
            data = new ArrayList<double[]>();
        }

        double[][] getData() {
            return data.toArray(new double[0][]);
        }

        int size() {
            return data.size();
        }

        void add(double x, double y) {
            data.add(new double[]{x,y});
        }

        double[] get(int i) {
            return data.get(i);
        }

        void addCircle() {
            final double[] start = data.get(data.size()-1);
            final double n = 2*Math.PI * sampleRate;

            for(int i = 0; i < n; ++i) {
                final double t = (i+1) * 2*Math.PI / n;
                data.add(new double[]{
                    start[0]+Math.sin(t),
                    start[1]+1-Math.cos(t)});
            }
        }

        void addRectangle(double w, double h) {
            final double[] start = data.get(data.size()-1);
            addLine(start[0]+w, start[1]);
            addLine(start[0]+w, start[1]+h);
            addLine(start[0], start[1]+h);
            addLine(start[0], start[1]);
        }

        void addLine(double x, double y) {
            final double[] start = data.get(data.size()-1);
            final double dx = x - start[0];
            final double dy = y - start[1];
            final double dist = Math.sqrt(dx*dx + dy*dy);
            final int n = (int)(dist * sampleRate);
            for(int i = 1; i <= n; ++i) {
                final double t = (double) i / n;
                data.add(new double[]{
                    start[0]+dx*t,
                    start[1]+dy*t
                });
            }
        }
    }


    @Test
    public void testSimpleDTW() throws Exception {

        double[][] q = {{0,1},{0,0},{1,0}}; // L shape

        double[][] d = {{50,50},{60,50},{60,40},{70,40},{80,40}}; // L starting at d[1]

        DynamicTimeWarping dtw = new DynamicTimeWarping(
                new double[][][]{q},
                new String[]{"L"},
                0.1, 1.0);

        for(int i = 0; i < d.length; ++i)
            dtw.observe(d[i]);
        while(dtw.observe(null));

        DynamicTimeWarping.Match m = dtw.getResult(false);
        assertNotNull(m);

        assertEquals(0.0, m.error, 1e-8);
        assertEquals(1, m.startPos());
        assertEquals(3, m.endPos());
        assertEquals("L", m.queryName());

        m = dtw.getResult(false);
        assertNull(m);
    }

    @Test
    public void testDTW() throws Exception {

        DrawData circle = new DrawData(10);
        circle.add(0,0);
        circle.addCircle();

        DrawData rectangle = new DrawData(10);
        rectangle.add(0,0);
        rectangle.addRectangle(1,1);

        ArrayList<double[][]> trainingExamples = new ArrayList<double[][]>();
        ArrayList<String> trainingExampleNames = new ArrayList<String>();

        trainingExamples.add(circle.getData());
        trainingExampleNames.add("circle");
        System.out.println("circle has " + circle.size() + " samples");

        trainingExamples.add(rectangle.getData());
        trainingExampleNames.add("rectangle");
        System.out.println("rectangle has " + rectangle.size() + " samples");

        DrawData data = new DrawData(10);
        data.add(20,10);
        data.addLine(25,10);

        ArrayList<Integer> trueStart = new ArrayList<Integer>();
        ArrayList<Integer> trueEnd = new ArrayList<Integer>();

        trueStart.add(data.size()-1); // -1 because the training examples start with a manually specified point, then add the remaining points.
        data.addRectangle(1,1);
        trueEnd.add(data.size()-1); // -1 because DynamicTimeWarping.Match.startPos() / endPos() index the first and last points belonging to the match - i.e. endPos() does not index the first point afterwards.

        data.addLine(30, 10);

        trueStart.add(data.size()-1);
        data.addCircle();
        trueEnd.add(data.size()-1);

        data.addLine(35,10);
        
        trueStart.add(data.size()-1);
        data.addCircle();
        trueEnd.add(data.size()-1);

//        int i = 0;
//        for(double[] p : circle) {
//            System.out.println(i++ +","+p[0]+","+p[1]);
//        }

//        for(int i = 0; i < data.size(); ++i)
//            System.out.println(i + "," + data.get(i)[0] + "," + data.get(i)[1]);


        DynamicTimeWarping dtw = new DynamicTimeWarping(
                trainingExamples.toArray(new double[0][][]),
//                trainingExampleNames.toArray(new String[0]), 0.0, 5.0);
                trainingExampleNames.toArray(new String[0]), 0.0, 1.0);

        for(int i = 0; i < data.size(); ++i)
            dtw.observe(data.get(i));
        while(dtw.observe(null));

        DynamicTimeWarping.Match m;
        for(int i=0; (m=dtw.getResult(false)) != null; ++i) {
            System.err.println(m);
            assertEquals(0.0, m.error, 1e-6);
            assertEquals(trueStart.get(i), (Integer) (int) m.startPos());
            assertEquals(trueEnd.get(i), (Integer) (int) m.endPos());
        }

        // test a training example against itself to ensure recognition can
        // occur without extra observations afterwards.
        dtw = new DynamicTimeWarping(
            new double[][][]{circle.getData()},
            new String[]{"circle"}, 0.1, 5.0);

        for(int i = 0; i < circle.size(); ++i)
            dtw.observe(circle.get(i));
        while(dtw.observe(null));

        m=dtw.getResult(false);
        assertNotNull(m);
        assertEquals(0.0, m.error, 1e-6);
        assertEquals(0, m.startPos());
        assertEquals(circle.size()-1, m.endPos());
    }

//
//  Currently it is DynamcTimeWarpingFE rather than DynamicTimeWarping
    //    that solves this problem, simply by rescaling the query.
//
//    @Test
//    public void testBigWindowDTW() throws Exception {
//
//        DrawData q = new DrawData(10);
//        q.add(0,0);
//        q.addLine(1,0);
//        q.addLine(2,1);
//        q.addLine(3,1);
//
//        // now the same thing at twice the speed.
//        DrawData d = new DrawData(20);
//        d.add(0,0);
//        d.addLine(1,0);
//        d.addLine(2,1);
//        d.addLine(3,1);
//
//        DynamicTimeWarping dtw = new DynamicTimeWarping(
//                new double[][][]{q.getData()},
//                new String[]{"ramp"}, 0.1, 1.0);
//
//        for(int i = 0; i < d.size(); ++i)
//            dtw.observe(d.get(i));
//        while(dtw.observe(null));
//
//        DynamicTimeWarping.Match m;
//        for(int i=0; (m=dtw.getResult(false)) != null; ++i) {
//            assertEquals(0.0, m.errorRate(), 1e-6); // it's the same path at a different sample rate so there should be almost no error.
////            assertEquals(trueStart.get(i), (Integer) (int) m.startPos());
////            assertEquals(trueEnd.get(i), (Integer) (int) m.endPos());
//        }
//
//    }
//

}
