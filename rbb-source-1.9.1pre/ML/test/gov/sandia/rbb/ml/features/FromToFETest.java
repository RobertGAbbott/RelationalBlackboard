/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.RBBML;
import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservationSequence;
import java.util.ArrayList;
import gov.sandia.rbb.Timeseries;
import gov.sandia.rbb.RBB;
import static gov.sandia.rbb.Tagset.TC;
import org.junit.Test;
import static org.junit.Assert.*;
import gov.sandia.rbb.RBBFilter;
import static gov.sandia.rbb.RBBFilter.*;

/**
 *
 * @author rgabbot
 */
public class FromToFETest {


    @Test
    public void testFromTo() throws Exception {

        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        final String jdbc = "jdbc:h2:mem:"+methodName;
        RBB rbb = RBB.create(jdbc, null);
        RBBML rbbml = new RBBML();
        rbbml.setRBBs("-rbb", jdbc);

        // create a scenario where x=3, x=6, and x=8 are constants.
        // then x=t moves from 5 down to 0 (thus passing 3) then
        // moves back up to 10 (thus passing 3, 6, and 8).

        // The correct result is fromTo should find a result
        // starting at t=3 (the second passing of 3) to t=6, then another from t=6 to 6=8

        Timeseries pt = new Timeseries(rbb, 1, -5.0, TC("x=t"));
        pt.add(rbb, -5.0, 5.0f);
        pt.add(rbb, 0.0, 0.0f);
        pt.add(rbb, 10.0, 10.0f);
        pt.setEnd(rbb.db(), 10.0);

        Timeseries t3 = new Timeseries(rbb, 1, -5.0, TC("x=3"));
        t3.add(rbb, 0.0, 3.0f);
        t3.setEnd(rbb.db(), 10.0);

        Timeseries t6 = new Timeseries(rbb, 1, -5.0, TC("x=6"));
        t6.add(rbb, 0.0, 6.0f);
        t6.setEnd(rbb.db(), 10.0);

        Timeseries t8 = new Timeseries(rbb, 1, -5.0, TC("x=8"));
        t8.add(rbb, 0.0, 8.0f);
        t8.setEnd(rbb.db(), 10.0);

        ArrayList<MLObservationSequence> results = new ArrayList<MLObservationSequence>();
        MLFeatureExtractor fe =
            new FromToFE("from", "to", "mover", "x", "x", 0.1f, null, null,
            new BufferObservationsFE(results,
            new PrintObservationFE("FromToFE: ", null)));
        rbbml.batch(
              new String[]{"mover"}, new RBBFilter[]{byTags("x=t")},
              new String[]{"x"}, new RBBFilter[]{byTags("x")},
              null, fe, null, null, 1.0, null);
        assertEquals(2, results.size());
        assertEquals(4, results.get(0).size()); // t=3-6 inclusive
        assertEquals(3, results.get(1).size()); // t=6-8 inclusive


        // now try with a maxDuration that excludes the second result.
        results = new ArrayList<MLObservationSequence>();
        fe =
            new FromToFE("from", "to", "mover", "x", "x", 0.1f, null, 2.5,
            new BufferObservationsFE(results,
            new PrintObservationFE("FromToFE: ", null)));
        rbbml.batch(
              new String[]{"mover"}, new RBBFilter[]{byTags("x=t")},
              new String[]{"x"}, new RBBFilter[]{byTags("x")},
              null, fe, null, null, 1.0, null);
        assertEquals(1, results.size());
        assertEquals(3, results.get(0).size()); // t=3-6 inclusive

        // now try with a minDuration that excludes the first result.
        results = new ArrayList<MLObservationSequence>();
        fe =
            new FromToFE("from", "to", "mover", "x", "x", 0.1f, 2.5, null,
            new BufferObservationsFE(results,
            new PrintObservationFE("FromToFE: ", null)));
        rbbml.batch(
              new String[]{"mover"}, new RBBFilter[]{byTags("x=t")},
              new String[]{"x"}, new RBBFilter[]{byTags("x")},
              null, fe, null, null, 1.0, null);
        assertEquals(1, results.size());
        assertEquals(4, results.get(0).size()); // t=3-6 inclusive

        rbb.disconnect();

   }
}