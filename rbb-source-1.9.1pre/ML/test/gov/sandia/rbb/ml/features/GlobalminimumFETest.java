/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

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
import gov.sandia.rbb.RBBFilter;
import static gov.sandia.rbb.RBBFilter.*;

/**
 *
 * @author rgabbot
 */
public class GlobalminimumFETest {

    public GlobalminimumFETest() {
    }

    @Test
    public void testGlobalMinimumFE() throws Exception {

        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        final String jdbc = "jdbc:h2:mem:"+methodName;
        RBB rbb = RBB.create(jdbc, null);
        RBBML rbbml = new RBBML();
        rbbml.setRBBs("-rbb", jdbc);


        Timeseries ts1 = new Timeseries(rbb, 1, 1.0, TC("n=1"));
        ts1.add(rbb, 1.0, 2.0f);
        ts1.add(rbb, 2.0, 6.0f);
        ts1.add(rbb, 3.0, 1.0f);
        ts1.add(rbb, 4.0, 2.0f);
        ts1.add(rbb, 5.0, 1.0f);
        ts1.setEnd(rbb.db(), 5.0);

        Timeseries ts2 = new Timeseries(rbb, 1, 4.0, TC("n=2"));
        ts2.add(rbb, 4.0, 7.0f);
        ts2.add(rbb, 5.0, 8.0f);
        ts2.add(rbb, 6.0, 9.0f);
        ts2.setEnd(rbb.db(), 6.0);

        ArrayList<MLObservationSequence> results = new ArrayList<MLObservationSequence>();

        String[] inputName = new String[]{"N"};

        MLFeatureExtractor fe =  new GlobalMinimumFE("N",
                    new BufferObservationsFE(results ,null));

        ResultSet rs = rbbml.batch(
              inputName, new RBBFilter[]{byTags("n")},
              null, null,
              null, fe, null, null, 1.0, null);

        assertEquals(2, results.size());

//        assertEquals(ts1.getID(), results.get(0).inputEvents[0].getID());
        assertEquals(ts1.getID(), results.get(0).getMetadata().getFeatureEvent("N").getID());
        assertEquals(3.0, results.get(0).getNewest().getTime(), 1e-6);
        assertEquals(1.0f, results.get(0).getNewest().getFeatureAsFloats("N")[0], 1e-6f);

        assertEquals(ts2.getID(), results.get(1).getMetadata().getFeatureEvent("N").getID());
        assertEquals(4.0, results.get(1).getNewest().getTime(), 1e-6);
        assertEquals(7.0f, results.get(1).getNewest().getFeatureAsFloats("N")[0], 1e-6f);

        rbbml.disconnect();
        rbb.disconnect();        
    }

}