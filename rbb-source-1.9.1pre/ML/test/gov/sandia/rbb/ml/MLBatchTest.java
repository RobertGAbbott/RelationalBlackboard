/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml;

import gov.sandia.rbb.impl.h2.statics.H2STime;
import gov.sandia.rbb.impl.h2.statics.H2SEvent;
import gov.sandia.rbb.RBB;
import gov.sandia.rbb.Timeseries;
import static gov.sandia.rbb.Tagset.TC;
import org.junit.Test;
import static org.junit.Assert.*;
import static gov.sandia.rbb.ml.RBBML.RBBMLMain;
import static gov.sandia.rbb.RBBFilter.*;

/**
 *
 * @author rgabbot
 */
public class MLBatchTest {

    @Test
    public void testWithGroups() throws Exception {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);
        String jdbc = "jdbc:h2:mem:"+methodName;
        RBB rbb = RBB.create(jdbc, null);


        // id=red1: y=0 from t=0 to t=4
        Timeseries ts = new Timeseries(rbb, 1, 0.0, TC("color=red,id=red1"));
        ts.add(rbb, 1.1, 0.0f);
        ts.setEnd(rbb.db(), 4.0);

        // id=red2: y=x from t=1 to t=4
        ts = new Timeseries(rbb, 1, 1.0, TC("color=red,id=red2"));
        ts.add(rbb, 1.0, 1.0f);
        ts.add(rbb, 4.0, 4.0f);
        ts.setEnd(rbb.db(), 4.0);

        // id=blue1: x-1 from t=1 to t=5
        ts = new Timeseries(rbb, 1, 1.0, TC("color=blue,id=blue1"));
        ts.add(rbb, 1.0, 0.0f);
        ts.add(rbb, 5.0, 4.0f);
        ts.setEnd(rbb.db(), 5.0);

        // id=blue2: y=2 from t=2 to t=5
        ts = new Timeseries(rbb, 1, 2.0, TC("color=blue,id=blue2"));
        ts.add(rbb, 2.0, 2.0f);
        ts.setEnd(rbb.db(), 5.0);

        /// test with a specified timestep
        RBBMLMain(
            "getFeatures",
            "-rbb", jdbc,
            "-input", "red", "color=red",
            "-group", "blues", "color=blue",
            "-timestep", "0.5",
            "-NearestFE", "positionOfNearestBlue:red:null:blues:1:false",
            "-DistanceFE", "distanceToNearestBlue:red:positionOfNearestBlue:SCALAR",
            "-CreateTimeseriesFE", "type=distanceToNearestBlue,red.id=red.id:distanceToNearestBlue:true:true"
        );

        Timeseries[] results = Timeseries.findWithSamples(rbb.db(), byTags("type=distanceToNearestBlue"));

        // 1 result for each distinct set of inputs = 2 since there are two reds.
        assertEquals(2, results.length);

        // check nr. samples for red1
        Timeseries[] t0 = Timeseries.findWithSamples(rbb.db(), byTags("red.id=red1,type=distanceToNearestBlue"));
        assertEquals(1, t0.length);
        ts = t0[0];
        assertEquals(7, ts.getNumSamples()); // red1 goes from t=0 to t=4, but the blue group is empty until t=1 and these are not stored, thus 7 samples from 1-4 with 0.5 timestep.
        // check values for red1
        // from x=[1,3] blue1 is closest at x-1.  This shows group members' positions are being interpolated.
        float x=1;
        assertEquals(x-1.0f, ts.valueLinear((double)x)[0], 1e-6f);
        x=1.2f;
        assertEquals(x-1.0f, ts.valueLinear((double)x)[0], 1e-6f);
        x=1.7f;
        assertEquals(x-1.0f, ts.valueLinear((double)x)[0], 1e-6f);
        // after that blue2 is closest at a constant distance of 2
        assertEquals(2.0f, ts.valueLinear(3.0)[0], 1e-6f);
        assertEquals(2.0f, ts.valueLinear(Math.PI)[0], 1e-6f);
        assertEquals(2.0f, ts.valueLinear(4.0)[0], 1e-6f);

        ///// now test without a specified timestep, so an observation is created at each time there is an observation for the first input.
        // first delete previous results.
        H2SEvent.delete(rbb.db(), byTags("type=distanceToNearestBlue"));
        RBBMLMain(
            "getFeatures",
            "-rbb", jdbc,
            "-input", "red", "color=red",
            "-group", "blues", "color=blue",
//            "-timestep", "0.5",
            "-NearestFE", "positionOfNearestBlue:red:null:blues:1:false",
            "-DistanceFE", "distanceToNearestBlue:red:positionOfNearestBlue:SCALAR",
            "-CreateTimeseriesFE", "type=distanceToNearestBlue,red.id=red.id:distanceToNearestBlue:true:true"
        );

        results = Timeseries.findWithSamples(rbb.db(), byTags("type=distanceToNearestBlue"));

        // 1 result for each distinct set of inputs = 2 since there are two reds.
        assertEquals(2, results.length);

        // check nr. samples for red1
        t0 = Timeseries.findWithSamples(rbb.db(), byTags("red.id=red1,type=distanceToNearestBlue"));
        assertEquals(1, t0.length);
        ts = t0[0];
        assertEquals(ts.getStart(), 0.0, 1e-6); // the resulting timeseries still extends over the life of red1, from 0 to 4.
        assertEquals(ts.getEnd(), 4.0, 1e-6);
        assertEquals(1, ts.getNumSamples()); // red1 was only observed once, at t=1.1
        assertEquals(1.1, ts.getSample(0).getTime(), 1e-6);

        rbb.disconnect();
    }

    @Test
    public void testWithTimeCoordinate() throws Exception {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);
        String jdbc = "jdbc:h2:mem:"+methodName;
        RBB rbb = RBB.create(jdbc, null);


        // id=red1: y=0 from t=0 to t=4
        double t0 = 1;
        Timeseries x = new Timeseries(rbb, 1, t0, TC("name=x,timeCoordinate=seconds"));
        x.add(rbb, t0, 0.0f);
        x.add(rbb, t0+1, 1.0f);
        x.setEnd(rbb.db(), t0+6.0);

        t0 = 11;
        Timeseries y = new Timeseries(rbb, 1, t0, TC("name=y,timeCoordinate=seconds"));
        y.add(rbb, t0, 0.0f);
        y.add(rbb, t0+1, 1.0f);
        y.setEnd(rbb.db(), t0+5.0);

        H2STime.defineCoordinate(rbb.db(), "timeCoordinate=seconds", 1, 0);
        H2SEvent.defineTimeCoordinatesForEventCombinations(rbb.db(), "secondsSinceStart", "name", null, 1.0);

        RBBMLMain(
            "getFeatures",
            "-rbb", jdbc,
            "-timeCoordinate", "timeCoordinate=secondsSinceStart",
            "-input", "x", "name=x",
            "-input", "y", "name=y",
            "-CreateTimeseriesFE", "name=z:x:true:true"
        );

        Timeseries[] t = Timeseries.findWithSamples(rbb.db(), byTags("name=z"));
        assertEquals(1, t.length);
        assertEquals(0.0, t[0].getStart(), 1e-8);
        assertEquals(5.0, t[0].getEnd(), 1e-8); // this is the shorter of the two durations.
        assertEquals(3.1f, t[0].valueLinear(3.1)[0], 1e-8f); // the z=f(x)=x

        rbb.disconnect();
    }

}
