/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml;

import gov.sandia.rbb.RBBFilter;
import gov.sandia.rbb.ml.features.CreateTimeseriesFE;
import gov.sandia.rbb.ml.features.BufferObservationsFE;
import java.util.ArrayList;
import gov.sandia.rbb.impl.h2.statics.H2SEvent;
import gov.sandia.rbb.RBB;
import gov.sandia.rbb.Timeseries;
import static gov.sandia.rbb.Tagset.TC;
import org.junit.Test;
import static org.junit.Assert.*;
import static gov.sandia.rbb.RBBFilter.*;

/**
 *
 * @author rgabbot
 */
public class MLOnlineTest {

    @Test
    public void towg() throws Exception {
        try { testOnlineWithGroups(); }
        catch(Exception e) {
            System.err.println(e);
            throw e;
        }
    }

    public void testOnlineWithGroups() throws Exception {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[2].getMethodName();
        System.err.println("Entering "+methodName);
        String jdbc = "jdbc:h2:mem:"+methodName;
        RBB rbb = RBB.create(jdbc, null);

        // description of the situation:
        // we will make an MLOnline that creates a problem instance for every combination of red and blue.
        // Then there will be a group input which is gray, whose numbers vary over time.

        // The reason this code is full of pauses (short sleeps) is because all the updates
        // are asynchronous event-driven (RBB EventListener)

        RBBML rbbml = new RBBML();
        rbbml.setRBBs("-rbb", jdbc);

        ArrayList<MLObservationSequence> obs = new ArrayList<MLObservationSequence>();
        MLOnline online = new MLOnline(rbbml,
            new String[]{"red", "blue"}, new RBBFilter[]{byTags("color=red"), byTags("color=blue")},
            new String[]{"grays"}, new RBBFilter[]{byTags("color=gray")},
            new BufferObservationsFE(obs,
              new CreateTimeseriesFE(TC("type=redcopy"), "red", true, true, null)),
            null);
      
        Timeseries red1 = new Timeseries(rbb, 1, 0.0, TC("color=red,id=red1"));
        Timeseries blue1 = new Timeseries(rbb, 1, 1.0, TC("color=blue,id=blue1"));
        pause();

        assertEquals(1, obs.size()); // 1 red/blue combination so far
        assertEquals(0, obs.get(0).size()); // but no observations yet.

        // now add an observation to the first input, red.
        // Note the other observation, blue, has no value yet, so it shouldn't create an observation.
        red1.add(rbb, 1.0, 10.0f);
        pause();
        assertEquals(1, obs.size()); // 1 red/blue combination so far
        assertEquals(0, obs.get(0).size()); // but no observations yet.

        // now add an observation to the second input, blue.
        // This will still not trigger an observation yet because blue is not the first input.
        blue1.add(rbb, 1.0, 20.0f);
        pause();
        assertEquals(1, obs.size()); // 1 red/blue combination so far
        assertEquals(0, obs.get(0).size()); // but no observations yet.

        // now create red2 and add a value to it.
        // This WILL trigger observation becase red is the first input.
        Timeseries red2 = new Timeseries(rbb, 1, 1.0, TC("color=red,id=red2"));
        red2.add(rbb, 1.0, 15.0f);
        pause();
        assertEquals(2, obs.size());
        assertEquals(1, obs.get(1).size()); // now 1 observation
        assertEquals(15.0f, obs.get(1).getNewest().getFeatureAsFloats("red")[0], 1e-6);
        assertEquals(20.0f, obs.get(1).getNewest().getFeatureAsFloats("blue")[0], 1e-6);
        assertEquals(0, obs.get(1).getNewest().getFeatureAsGroup("grays").getNumFeatures()); // the grays group is empty so far (but not null)
        // CreateTimeseriesFE is writing the copy of red back to the RBB
        assertEquals(1, Timeseries.findWithSamples(rbb.db(), byTags("type=redcopy")).length);

        // now let's make sure values are being interpolated/extrapolated.
        blue1.add(rbb, 2.0, 30.0f); // blue was 20 at 1.0 and has a slope of 10
        red2.add(rbb, 3.0, 19.0f); // red was 15 at 1.0 and has a slope of 2.  This also will trigger an observation.
        pause();
        assertEquals(2, obs.size()); // 1 red/blue combination so far
        assertEquals(2, obs.get(1).size()); // now 2 observations.
        assertEquals(3.0, obs.get(1).getNewest().getTime(), 1e-6); // the observation is at the time input 0 was updated.
        assertEquals(19.0f, obs.get(1).getNewest().getFeatureAsFloats("red")[0], 1e-6);
        assertEquals(40.0f, obs.get(1).getNewest().getFeatureAsFloats("blue")[0], 1e-6); // this value is extrapolated.
        assertEquals(0, obs.get(1).getNewest().getFeatureAsGroup("grays").getNumFeatures()); // the grays group is empty so far (but not null)

        // now try adding something to the group.
        Timeseries gray1 = new Timeseries(rbb, 1, 4.0, TC("color=gray,id=gray1"));
        // try an observation now that grays is non-empty, but the only member in it has no value.
        red2.add(rbb, 4.0, 20.0f);
        pause();
        assertEquals(2, obs.size()); // 1 red/blue combination so far
        assertEquals(3, obs.get(1).size());
        assertEquals(4.0, obs.get(1).getNewest().getTime(), 1e-6); // the observation is at the time input 0 was updated.
        assertEquals(20.0f, obs.get(1).getNewest().getFeatureAsFloats("red")[0], 1e-6);
        assertEquals(50.0f, obs.get(1).getNewest().getFeatureAsFloats("blue")[0], 1e-6); // this value is extrapolated.
        assertEquals(0, obs.get(1).getNewest().getFeatureAsGroup("grays").getNumFeatures()); // the grays group is empty so far (but not null)

        // now a non-empty group.
        gray1.add(rbb, 5.0, 555.0f);
        red2.add(rbb, 5.0, 20.0f);
        pause();
        assertEquals(2, obs.size()); // 1 red/blue combination so far
        assertEquals(4, obs.get(1).size());
        assertEquals(5.0, obs.get(1).getNewest().getTime(), 1e-6); // the observation is at the time input 0 was updated.
        assertEquals(20.0f, obs.get(1).getNewest().getFeatureAsFloats("red")[0], 1e-6);
        assertEquals(60.0f, obs.get(1).getNewest().getFeatureAsFloats("blue")[0], 1e-6); // this value is extrapolated.
        assertEquals(1, obs.get(1).getNewest().getFeatureAsGroup("grays").getNumFeatures()); // finally somebody is in the grays group.
        assertEquals(555.0f, obs.get(1).getNewest().getFeatureAsGroup("grays").getFeatureAsFloats(gray1.getID().toString())[0], 1e-6f); // finally somebody is in the grays group.

        // ensure group member values are interpolated/extrapolated
        gray1.add(rbb, 5.5, 555.5f); // has a slope of 1
        red2.add(rbb, 6.0, 20.0f);
        pause();
        assertEquals(556.0f, obs.get(1).getNewest().getFeatureAsGroup("grays").getFeatureAsFloats(gray1.getID().toString())[0], 1e-6f); // this value is extrapolated.

        // add a second group member to ensure they don't interfere, including when one is ended.
        Timeseries gray2 = new Timeseries(rbb, 1, 6.0, TC("color=gray,id=gray2"));
        gray2.add(rbb, 6.0, 3.14f);
        pause(); // without this pause, the observation created by the next line may well not see that gray2 had a value added to it.
        red2.add(rbb, 7.0, 20.0f);
        pause();
        assertEquals(2, obs.get(1).getNewest().getFeatureAsGroup("grays").getNumFeatures()); 
        assertEquals(557.0f, obs.get(1).getNewest().getFeatureAsGroup("grays").getFeatureAsFloats(gray1.getID().toString())[0], 1e-6f); // this value is extrapolated.
        assertEquals(3.14f, obs.get(1).getNewest().getFeatureAsGroup("grays").getFeatureAsFloats(gray2.getID().toString())[0], 1e-6f); // this value is extrapolated.

        gray2.setEnd(rbb.db(), 7.5);
        pause();
        red2.add(rbb, 8.0, 20.0f);
        pause();
        assertEquals(1, obs.get(1).getNewest().getFeatureAsGroup("grays").getNumFeatures()); // now we're back to 1 group member.
        assertEquals(558.0f, obs.get(1).getNewest().getFeatureAsGroup("grays").getFeatureAsFloats(gray1.getID().toString())[0], 1e-6f); // this value is extrapolated.

        // creating blue2 creates two new problem instances - one for each of red1 and red2.
        assertEquals(2, obs.size());
        Timeseries blue2 = new Timeseries(rbb, 1, 8.0, TC("color=blue,id=blue2"));
        pause();
        assertEquals(4, obs.size());


        // end all timeseries.
        H2SEvent.setEnd(rbb.db(), "color", 50.0);

        pause();

        rbbml.disconnect();
        rbb.disconnect();
    }

    private static void pause() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException ex) {
        }
    }
}
