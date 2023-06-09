/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;


/**
 * A Segmentation splits one sequence of observations into several.
 * It calls its successors in the chain only if a test passes.
 * The test is administered on the Observations individually, and
 * a new init/observe.../done cycle is done for each continuous run
 * of passed tests.
 *
 */
public abstract class SegmentationFE extends MLFeatureExtractor
{
    public abstract boolean gateTest(MLObservation obs) throws Exception;

    private boolean wasOpen=false;
    private Double prevTime;

    public SegmentationFE(MLFeatureExtractor next)
    {
        super(next);
    }
    
    private void closeGate(double endedByTime) throws Exception
    {
        if(!wasOpen)
            return;

        // the score fell below the threshold sometime between the previous observation
        // and now, so just split the difference.
        super.done((this.prevTime+endedByTime)/2);

        wasOpen = false;

    }

    @Override
    public void done(double time) throws Exception {
        closeGate(time);

        // do not call super.done()
        // closeGate will do so if a flag was in progress.
    }

    @Override
    public void init(double time, MLObservation.Metadata md) throws Exception {
        prevTime = time;
        wasOpen=false;
        //// do not call super.init at this time.
    }

    @Override
    public void observe(MLObservation obs) throws Exception {
        
        final boolean isOpen = gateTest(obs);

        // see if we need to open the gate
        if(isOpen && !wasOpen)
        {
            // we can't say the exact time when the test would have become true given
            // a continuous input stream.
            // Assuming it was only just at the time of the current observation results in a 0-duration
            // event when the test passes for only a single observation.
            // So instead assume it was halfway between this and the previous observation.
            double startTime = (obs.getTime()+this.prevTime)/2;

            if(super.nextFeatureExtractor != null)
                super.nextFeatureExtractor.init(startTime, obs.getMetadata());
        }
        // see if we need to close the gate
        else if(wasOpen && !isOpen)
        {
            closeGate(obs.getTime());
        }

        if(isOpen)
            super.observe(obs);

        prevTime = obs.getTime();
        wasOpen = isOpen;
  }

}
