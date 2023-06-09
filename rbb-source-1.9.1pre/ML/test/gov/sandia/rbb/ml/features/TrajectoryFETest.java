/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLObservation.Metadata;
import gov.sandia.rbb.ml.MLObservation;
import gov.sandia.rbb.ml.MLObservationSequence;
import java.util.ArrayList;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author rgabbot
 */
public class TrajectoryFETest {

    @Test
    public void trajectoryFETest() throws Exception {

        // the trajectory will be downsampled to this many.
        for(int len = 2; len <= 7; ++len) {

            ArrayList<MLObservationSequence> allObs = new ArrayList<MLObservationSequence>();
            TrajectoryFE hist = new TrajectoryFE("xyHistory", "xy", len,
                    new BufferObservationsFE(allObs,
                    new PrintObservationFE("",null)));

            Metadata md = new Metadata();
            md.add("xy", null);
            hist.addSelfToFeatureNames(md);

            hist.init(0.0, md);

            final int n = 5;

            for(int i = 0; i < n; ++i) {
                Float[] xy = new Float[2];
                xy[0] = (float) i;
                xy[1] = (float) 10+i;
                MLObservation obs = new MLObservation(i, md);
                obs.setFeatureAsFloats("xy", xy);
                hist.observe(obs);
            }

            hist.done(n);

            assertEquals(1, allObs.size());
            MLObservationSequence seq = allObs.get(0);
            assertEquals(n, seq.size());

            for(int i = 0; i < n; ++i) {
                Float[] x = seq.getOldest(i).getFeatureAsFloats("xyHistory");
                assertEquals(x.length, 2 * len); // 2* since the data is 2d
                assertEquals(x[0], 0.0f, 1e-6f);
                assertEquals(x[x.length-2], n-1, 1e-6f); // -2 is x component of last sample
                for(int j = 1; j < len; ++j) {
                    assertTrue(x[j*2] >= x[(j-1)*2]); // evens are x
                    assertTrue(x[j*2+1] >= x[(j-1)*2+1]); // odds are y
                    assertEquals(x[j*2], x[j*2+1]-10.0f, 1e-6);
                }
            }

        }
    }
 
}