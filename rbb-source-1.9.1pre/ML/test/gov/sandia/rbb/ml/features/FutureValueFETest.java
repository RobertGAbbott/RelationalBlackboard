/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLObservation.Metadata;
import java.util.ArrayList;
import gov.sandia.rbb.ml.MLObservationSequence;
import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author rgabbot
 */
public class FutureValueFETest {
    @Test
    public void testFutureValueFE() throws Exception {
        ArrayList<MLObservationSequence> outputArray = new ArrayList<MLObservationSequence>();
        MLFeatureExtractor fe =
                new FutureValueFE("x1", "x", 1,
                new FutureValueFE("x2", "x", 2,
                new BufferObservationsFE(outputArray, null)));

        Metadata fn = new Metadata();
        fn.add("x", null);
        fe.addSelfToFeatureNames(fn);


        fe.init(1.0, null);
        
        for(int i = 2; i <= 6; ++i)
            fe.observe(new MLObservation((double)i, fn, new Object[]{new Float[]{(float)i}}));

        fe.done(6);

        assertEquals(1, outputArray.size());
        MLObservationSequence output = outputArray.get(0);
        assertEquals(2, output.size());

        assertEquals(3, output.getOldest().getNumFeatures());
        assertEquals(2.0f, output.getOldest().getFeatureAsFloats("x")[0], 1e-6f);
        assertEquals(3.0f, output.getOldest().getFeatureAsFloats("x1")[0], 1e-6f);
        assertEquals(4.0f, output.getOldest().getFeatureAsFloats("x2")[0], 1e-6f);

        assertEquals(3, output.getOldest(1).getNumFeatures());
        assertEquals(3.0f, output.getOldest(1).getFeatureAsFloats("x")[0], 1e-6f);
        assertEquals(4.0f, output.getOldest(1).getFeatureAsFloats("x1")[0], 1e-6f);
        assertEquals(5.0f, output.getOldest(1).getFeatureAsFloats("x2")[0], 1e-6f);

    }
}

