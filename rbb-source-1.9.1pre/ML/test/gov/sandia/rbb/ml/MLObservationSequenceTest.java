/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml;

import java.sql.SQLException;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author rgabbot
 */
public class MLObservationSequenceTest {
    @Test
    public void testAccumulator() throws SQLException
    {
        MLObservation.Metadata md = new MLObservation.Metadata();
        md.add("x", null);
        MLObservationSequence d = new MLObservationSequence(5, 100.0, md);

        // do observations that will be bounded by the max size of 5.
        for(int i = 0; i < 200; ++i)
            d.addObservation(new MLObservation(i, md, (Object)new Float[]{(float)i}));
        
        assertEquals(5, d.size());
        assertEquals(195.0, d.getOldest().getTime(), 1e-6);

        // do observations that will be bounded by the max age of 100
        d = new MLObservationSequence(500, 100.0, md);
        for(int i = 0; i < 200; ++i)
            d.addObservation(new MLObservation(i, md, (Object) new Float[]{(float)(i*10)}));

        assertEquals(101, d.size());
        assertEquals(99.0, d.getOldest().getTime(), 1e-6);
        assertEquals(1990.0f, d.getNewest(0).getFeatureAsFloats("x")[0], 1e-6f);
        assertEquals(990.0f, d.getNewest(100).getFeatureAsFloats("x")[0], 1e-6f);
    }


    private void observe(MLObservationSequence seq, double t, String feature, Float... x) {
        MLObservation ob = new MLObservation(t,seq.metadata);
        ob.setFeatureAsFloats(feature, x);
        seq.addObservation(ob);
    }

    @Test
    public void testInterpolateFeatureAsFloats() {
        MLObservation.Metadata md = new MLObservation.Metadata();
        md.add("x", null);
        MLObservationSequence seq = new MLObservationSequence(null, null, md);

        assert(null == seq.interpolateFeatureAsFloats("x", 20.0)); // no observations
        
        observe(seq,  5.0, "x", 25.0f);  // y=5*t for t <= 10

        assertEquals(25.0f, seq.interpolateFeatureAsFloats("x", 1234.0)[0], 1e-6f); // one value = constant interpolation value.

        observe(seq, 10.0, "x", 50.0f); 
        observe(seq, 20.0, "x", 200.0f); // y=10*t from t=20 to t=25
        observe(seq, 25.0, "x", 250.0f);
        observe(seq, 30.0, "x", 150.0f); // y = 5*t again for t >= 30
        observe(seq, 31.0, "x", 155.0f); 

        assertEquals(200.0f, seq.interpolateFeatureAsFloats("x", 20.0)[0], 1e-6f); // exact time
        assertEquals(210.0f, seq.interpolateFeatureAsFloats("x", 21.0)[0], 1e-6f); // between two values
        assertEquals(5.0f, seq.interpolateFeatureAsFloats("x", 1.0)[0], 1e-6f); // before first
        assertEquals(500.0f, seq.interpolateFeatureAsFloats("x", 100.0)[0], 1e-6f); // after last
       
    }

}
