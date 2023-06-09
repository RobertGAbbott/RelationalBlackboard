/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLObservationSequence;
import java.util.ArrayList;
import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author rgabbot
 */
public class TurnRateFETest {
    @Test
    public void turnRateFETest() throws Exception {
        ArrayList<MLObservationSequence> obs = new ArrayList<MLObservationSequence>();
        MLFeatureExtractor turnRate = new TurnRateFE("turnRate", "position",
            new BufferObservationsFE(obs, null));

        MLObservation.Metadata md = new MLObservation.Metadata();
        md.add("position", null);
        md.add("turnRate", null);

        turnRate.init(0.0, null);
        ObservePos ob = new ObservePos(md, turnRate);

        float x = 0.0f, y=0.0f;
        double t = 0.0;
        int n = 0;
        ob.observe(t, x, y);
        ob.observe(++t, ++x, y); // move to right
        ob.observe(++t, x, ++y); // move up (left turn)
        // after 3 observations you getFeatureName the original value 3 times.
        assertEquals(Math.PI/2.0, (double) obs.get(0).getOldest(n++).getFeatureAsFloats("turnRate")[0], 1e-6);
        assertEquals(Math.PI/2.0, (double) obs.get(0).getOldest(n++).getFeatureAsFloats("turnRate")[0], 1e-6);
        assertEquals(Math.PI/2.0, (double) obs.get(0).getOldest(n++).getFeatureAsFloats("turnRate")[0], 1e-6);

        ob.observe(++t, ++x, y); // move right (right turn)
        assertEquals(-Math.PI/2.0, (double) obs.get(0).getOldest(n++).getFeatureAsFloats("turnRate")[0], 1e-6);

        ob.observe(t+=0.5, x, --y); // move down in half the time (right turn at twice the rate)
        assertEquals(-Math.PI, (double) obs.get(0).getOldest(n++).getFeatureAsFloats("turnRate")[0], 1e-6);

        ob.observe(++t, --x, y); // move left (turn right)... make sure the sign depends on direction of turn and not y component.
        assertEquals(-Math.PI/2.0, (double) obs.get(0).getOldest(n++).getFeatureAsFloats("turnRate")[0], 1e-6);
    }

    static class ObservePos {
        MLObservation.Metadata fn;
        MLFeatureExtractor fe;
        ObservePos(MLObservation.Metadata md, MLFeatureExtractor fe) { this.fn=md; this.fe=fe; }
        void observe(double t, float x, float y)  throws Exception {
            MLObservation o = new MLObservation(t, fn);
            o.setFeature("position", new Float[]{x,y});
            fe.observe(o);
        }
    }
 
}