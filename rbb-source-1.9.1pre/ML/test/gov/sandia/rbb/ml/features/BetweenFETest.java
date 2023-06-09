/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.RBBFilter;
import static gov.sandia.rbb.RBBFilter.*;
import gov.sandia.rbb.ml.RBBML;
import static gov.sandia.rbb.Tagset.TC;
import java.sql.ResultSet;
import gov.sandia.rbb.ml.MLObservationSequence;
import java.util.ArrayList;
import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.Timeseries;
import gov.sandia.rbb.RBB;
import org.junit.Test;
import static org.junit.Assert.*;

public class BetweenFETest {
    @Test
    public void testBetween() throws Exception {

        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        final String jdbc = "jdbc:h2:mem:"+methodName;
        RBB rbb = RBB.create(jdbc, null);
        RBBML rbbml = new RBBML();
        rbbml.setRBBs("-rbb", jdbc);

        double t = 0;
        float x = (float) 0.1f;
        Timeseries ts1 = new Timeseries(rbb, 1, t, TC("n=1"));
        for(; t <= 3; ++t, ++x)
            ts1.add(rbb, t, x);
        ts1.setEnd(rbb.db(), t); // note the end time is 4 - 1 after the final observation.

        ArrayList<MLObservationSequence> results = new ArrayList<MLObservationSequence>();

        MLFeatureExtractor fe =
              //  new PrintObservationFE("all: ",
                new BetweenFE("N", 2.0f, 14.0f,
                    new BufferObservationsFE(results,
                    new PrintObservationFE("between 2,14: ", null)));

        ResultSet rs = rbbml.batch(
              new String[]{"N"}, new RBBFilter[]{byTags("n")},
              null, null,
              null, fe, null, null, 1.0, null);

        assertEquals(1, results.size());
        assertEquals(3, results.get(0).size());

        assertEquals(2.0, results.get(0).getOldest(0).getTime(), 1e-6);
        assertEquals(2.1f, results.get(0).getOldest(0).getFeatureAsFloats("N")[0], 1e-6f);

        assertEquals(2.0, results.get(0).getOldest(0).getTime(), 1e-6);
        assertEquals(3.1f, results.get(0).getOldest(1).getFeatureAsFloats("N")[0], 1e-6f);

        assertEquals(2.0, results.get(0).getOldest(0).getTime(), 1e-6);
        assertEquals(4.1f, results.get(0).getOldest(2).getFeatureAsFloats("N")[0], 1e-6f);

//        assertEquals(3.0, results.get(0).getNewest().getTime(), 1e-6);

        rbbml.disconnect();
        rbb.disconnect();        
    }

}