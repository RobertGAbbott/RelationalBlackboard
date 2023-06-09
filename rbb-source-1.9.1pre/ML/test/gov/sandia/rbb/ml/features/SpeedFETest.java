/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLObservation;
import gov.sandia.rbb.ml.MLObservationSequence;
import java.util.ArrayList;
import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.RBB;
import org.junit.Test;
import static org.junit.Assert.*;

public class SpeedFETest {
    @Test
    public void testSpeed() throws Exception {

        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);


        ArrayList<MLObservationSequence> results = new ArrayList<MLObservationSequence>();

        MLFeatureExtractor fe =
              //  new PrintObservationFE("all: ",
                new SpeedFE("speed", "xy",
                    new BufferObservationsFE(results, null));

        MLObservation.Metadata md = new MLObservation.Metadata();
        md.add("xy", null);
        fe.addSelfToFeatureNames(md);
        fe.init(1, md);

        MLObservation ob = new MLObservation(1, md);
        ob.setFeatureAsFloats("xy", 0.0f, 0.0f);
        fe.observe(ob);

        MLObservation ob2= new MLObservation(1.5, md);
        ob2.setFeatureAsFloats("xy", 0.0f, 1.0f); // 1 unit distance in 0.5 unit time; speed =2
        fe.observe(ob2);

        MLObservation ob3= new MLObservation(2.5, md); // 2 unit distance in 1 unit time; speed = 2
        ob3.setFeatureAsFloats("xy", 2.0f, 1.0f);
        fe.observe(ob3);

        assertEquals(1, results.size());
        assertEquals(3, results.get(0).size());

        assertEquals(1.0, results.get(0).getOldest(0).getTime(), 1e-6);
        assertEquals(2.0f, results.get(0).getOldest(0).getFeatureAsFloats("speed")[0], 1e-6f);

        assertEquals(1.5, results.get(0).getOldest(1).getTime(), 1e-6);
        assertEquals(2.0f, results.get(0).getOldest(1).getFeatureAsFloats("speed")[0], 1e-6f); // initial speed repeated for two observations

        assertEquals(2.5, results.get(0).getOldest(2).getTime(), 1e-6);
        assertEquals(2.0f, results.get(0).getOldest(2).getFeatureAsFloats("speed")[0], 1e-6f);

//        assertEquals(3.0, results.get(0).getNewest().getTime(), 1e-6);

        rbb.disconnect();        
    }

}