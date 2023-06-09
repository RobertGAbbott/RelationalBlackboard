/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml;

import gov.sandia.rbb.RBB;
import gov.sandia.rbb.Timeseries;
import static gov.sandia.rbb.Tagset.TC;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author rgabbot
 */
public class RBBMLTest {

    @Test
    public void testMakeObservationTimes() throws Exception {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);
        String jdbc = "jdbc:h2:mem:"+methodName;
        RBB rbb = RBB.create(jdbc, null);

        final double problemStart = 0.5;
        final double problemEnd = 4.5;
        Timeseries ts = new Timeseries(rbb, 1, problemStart, TC(""));
        ts.add(rbb, 1, 1.0f);
        ts.add(rbb, 2, 2.0f);
        ts.add(rbb, 3, 3.0f);
        ts.add(rbb, 4, 4.0f);
        ts.setEnd(rbb.db(), problemEnd);

        //////////// observe point in time.

        // basic case of view at a single specified time with no warmup or cooldown.
        Double[] t = RBBML.makeObservationTimes(rbb, ts.getID(),
            problemStart, problemEnd, // timeseries exist start/end
            2.5, 2.5, // time of interest start/end
            0.0, 0.0, // no warmup/cooldown
            null, // don't resample
            null, null); // no time coordinate
        assertEquals(1, t.length);
        assertEquals(2.5, t[0], 1e-6);

        // with a warmup
        t = RBBML.makeObservationTimes(rbb, ts.getID(),
            problemStart, problemEnd, // timeseries exist start/end
            2.5, 2.5, // time of interest start/end
            1.0, 0.0, // warmup/cooldown
            null, // don't resample
            null, null); // no time coordinate
        assertEquals(3, t.length);
        assertEquals(1.0, t[0], 1e-6);
        assertEquals(2.0, t[1], 1e-6);
        assertEquals(2.5, t[2], 1e-6);

        // with a cooldown
        t = RBBML.makeObservationTimes(rbb, ts.getID(),
            problemStart, problemEnd, // timeseries exist start/end
            1.5, 1.5, // time of interest start/end
            0.0, 1.0, // warmup/cooldown
            null, // don't resample
            null, null); // no time coordinate
        assertEquals(3, t.length);
        assertEquals(1.5, t[0], 1e-6);
        assertEquals(2.0, t[1], 1e-6);
        assertEquals(3.0, t[2], 1e-6);

        // with indefinite warmup
        t = RBBML.makeObservationTimes(rbb, ts.getID(),
            problemStart, problemEnd, // timeseries exist start/end
            2.5, 2.5, // time of interest start/end
            null, 0.0, // warmup/cooldown
            null, // don't resample
            null, null); // no time coordinate
        assertEquals(3, t.length);
        assertEquals(1.0, t[0], 1e-6);
        assertEquals(2.0, t[1], 1e-6);
        assertEquals(2.5, t[2], 1e-6);

        // with indefinite cooldown
        t = RBBML.makeObservationTimes(rbb, ts.getID(),
            problemStart, problemEnd, // timeseries exist start/end
            2.5, 2.5, // time of interest start/end
            0.0, null, // warmup/cooldown
            null, // don't resample
            null, null); // no time coordinate
        assertEquals(3, t.length);
        assertEquals(2.5, t[0], 1e-6);
        assertEquals(3.0, t[1], 1e-6);
        assertEquals(4.0, t[2], 1e-6);

        //////// observe time period, without resampling.

        // simple case
        t = RBBML.makeObservationTimes(rbb, ts.getID(),
            problemStart, problemEnd, // timeseries exist start/end
            2.0, 3.0, // time of interest start/end
            0.0, 0.0, // no warmup/cooldown
            null, // don't resample
            null, null); // no time coordinate
        assertEquals(2, t.length);
        assertEquals(2.0, t[0], 1e-6);
        assertEquals(3.0, t[1], 1e-6);

        // with warmup
        t = RBBML.makeObservationTimes(rbb, ts.getID(),
            problemStart, problemEnd, // timeseries exist start/end
            3.0, 4.0, // time of interest start/end
            1.0, 0.0, // warmup/cooldown
            null, // don't resample
            null, null); // no time coordinate
        assertEquals(3, t.length);
        assertEquals(2.0, t[0], 1e-6);
        assertEquals(3.0, t[1], 1e-6);
        assertEquals(4.0, t[2], 1e-6);

        // with cooldown
        t = RBBML.makeObservationTimes(rbb, ts.getID(),
            problemStart, problemEnd, // timeseries exist start/end
            2.0, 3.0, // time of interest start/end
            0.0, 1.0, // warmup/cooldown
            null, // don't resample
            null, null); // no time coordinate
        assertEquals(3, t.length);
        assertEquals(2.0, t[0], 1e-6);
        assertEquals(3.0, t[1], 1e-6);
        assertEquals(4.0, t[2], 1e-6);

        // with indefinite warmup
        t = RBBML.makeObservationTimes(rbb, ts.getID(),
            problemStart, problemEnd, // timeseries exist start/end
            2.0, 3.0, // time of interest start/end
            null, 0.0, // warmup/cooldown
            null, // don't resample
            null, null); // no time coordinate
        assertEquals(3, t.length);
        assertEquals(1.0, t[0], 1e-6);
        assertEquals(2.0, t[1], 1e-6);
        assertEquals(3.0, t[2], 1e-6);

        // with indefinite cooldown
        t = RBBML.makeObservationTimes(rbb, ts.getID(),
            problemStart, problemEnd, // timeseries exist start/end
            1.5, 2.5, // time of interest start/end
            0.0, null, // warmup/cooldown
            null, // don't resample
            null, null); // no time coordinate
        assertEquals(3, t.length);
        assertEquals(2.0, t[0], 1e-6);
        assertEquals(3.0, t[1], 1e-6);
        assertEquals(4.0, t[2], 1e-6);


        //////// observe time period, with resampling.

        // simple case
        t = RBBML.makeObservationTimes(rbb, ts.getID(),
            problemStart, problemEnd, // timeseries exist start/end
            2.1, 3.1, // time of interest start/end
            0.0, 0.0, // warmup/cooldown
            0.5, // resample
            null, null); // no time coordinate
        assertEquals(3, t.length);
        assertEquals(2.1, t[0], 1e-6);
        assertEquals(2.6, t[1], 1e-6);
        assertEquals(3.1, t[2], 1e-6);

        // no start time of interest, lower bound not a multiple of resampleTimestep
        t = RBBML.makeObservationTimes(rbb, ts.getID(),
            2.1, problemEnd, // timeseries exist start/end
            null, 3.1, // time of interest start/end
            0.0, 0.0, // warmup/cooldown
            0.5, // resample
            null, null); // no time coordinate
        assertEquals(2, t.length);
        assertEquals(2.5, t[0], 1e-6);
        assertEquals(3.0, t[1], 1e-6);

        // warmup is tiny, but lowerBound allows room to expand it so warmup will be 
        // exceeded rather than discarded, keeping all samples on multiples of the timestep.
        t = RBBML.makeObservationTimes(rbb, ts.getID(),
            problemStart, problemEnd, // timeseries exist start/end
            1.0, 1.5, // time of interest start/end
            1e-6, 0.0, // warmup/cooldown
            0.5, // resample
            null, null); // no time coordinate
        assertEquals(3, t.length);
        assertEquals(0.5, t[0], 1e-6);
        assertEquals(1.0, t[1], 1e-6);
        assertEquals(1.5, t[2], 1e-6);

        // force it to sacrifice warmup to stay above lowerBound
        t = RBBML.makeObservationTimes(rbb, ts.getID(),
            problemStart, problemEnd, // timeseries exist start/end
            0.75, 1.5, // time of interest start/end
            0.3, 0.0, // warmup/cooldown
            0.5, // resample
            null, null); // no time coordinate
        assertEquals(2, t.length);
        assertEquals(0.75, t[0], 1e-6);
        assertEquals(1.25, t[1], 1e-6);

        // try an invalid set of paramters
        try {
        t = RBBML.makeObservationTimes(rbb, ts.getID(),
            3.1, problemEnd, // timeseries exist start/end
            null, 3.2, // time of interest start/end
            0.0, 0.0, // warmup/cooldown
            0.5, // resample
            null, null); // no time coordinate
            assertTrue(false); // should throw exception before getting here; first observation would be 3.5 which is after time of interest.
        }
        catch(Exception e) {
            
        }

        rbb.disconnect();
    }

}
