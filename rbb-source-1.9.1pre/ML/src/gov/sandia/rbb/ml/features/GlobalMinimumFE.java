/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;
import gov.sandia.rbb.RBB;


/**
 * Passes observation down the chain only once for each problem instance -
 * the global minimum input.
 * "Global" in this case means "anywhere in the single problem instance"
 * Only dimension 0 is used.
 * If multiple equal global minima exist the first is used.
 *
 * @author rgabbot
 */
public class GlobalMinimumFE extends MLFeatureExtractor {

    MLObservation min;
    String input;

    public GlobalMinimumFE(String input, MLFeatureExtractor nextFeatureExtractor)
    {
        super(nextFeatureExtractor);
        this.input = input;
    }


    @Override
    public void init(double time, MLObservation.Metadata md) throws Exception {
        min = null;

        //// do not call super.init at this time.
        //// The successor will only be initialized by done(), when the global minimum is known.
    }

    @Override
    public void observe(MLObservation obs) throws Exception {
        if(min == null || obs.getFeatureAsFloats(input)[0] < min.getFeatureAsFloats(input)[0])
            min = obs;
        // do not call super.observe()
    }

    @Override
    public void done(double time) throws Exception {
        if(min != null) {
            super.init(min.getTime(), min.getMetadata());
            super.observe(min);
            super.done(min.getTime());
        }
    }
}
