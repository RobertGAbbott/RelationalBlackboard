
package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;
import gov.sandia.rbb.ml.MLObservationSequence;
import gov.sandia.rbb.Event;
import gov.sandia.rbb.RBB;


/**
 *
 * Insert a feature that is a past or future value of another feature.
 *
 * Future values are obtained by stalling feature extraction in this stage as long as necessary.
 *
 * Each FutureValueFE imposes a latency of 'future' on the chain, and will 
 * cause 'future' observations at the end of the sequence to be lost.
 *
 * The effect is additive, so if it is necessary to delay several features, it would
 * be better to extend this class to handle multiple features, rather than stacking these.
 *
 * Unlike SmoothingFE, the output is a discrete past or future value, not a combination of them.
 * Unlike FeedbackFE, this FE produces future values by stalling feature extraction in this stage as long as necessary,
 * instead of by using an exogenous input.
 *
 * @author rgabbot
 */
public class FutureValueFE extends MLFeatureExtractor {

    private int future;

    private MLObservationSequence data;

    private String input;

    private Double initTime;

    /**
     * If future is positive, the output is the value of the input from this many observations ahead of time.
     * e.g. future=1 outputs the immediate successor
     * The last 'future' observations will be discarded (make sure the warmupCooldown number in your Model is large enough).
     *
     */
    public FutureValueFE(String output, String input, int future, MLFeatureExtractor next)
    {
        super(next, output);
        this.input = input;


        if(future <= 0) {
            // A negative 'future' value is not supported.
            throw new UnsupportedOperationException();
        }

        this.future = future;
    }

    @Override
    public void init(double time, MLObservation.Metadata md) throws Exception {

        this.data = new MLObservationSequence(future, null, md);

        //// delay calling init until (unless) we have enough past or future values.
        // super.init(rbb, time, md);
        initTime = time;
    }

    @Override
    public void observe(MLObservation obs) throws Exception {

        if(future > 0 && data.size() == future) {
            if(initTime != null) {
                super.init(initTime, obs.getMetadata());
                initTime = null;
            }
            MLObservation ready = data.getOldest();
            ready.setFeature(this.getOutputName(0), obs.getFeature(input));
            super.observe(ready);
        }

        // since we set a max size this will evict old Observations when necessary.
        data.addObservation(obs);
    }
}
