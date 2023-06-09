/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;


/**
 *
 * FeedbackFE creates a feature from the previous value of another feature.
 * The other feature may be after this one in the chain.
 * The value for the initial observation is an external input (i.e. a constructor arg)
 * If the initial value is specified as null, no observation is generated until the second observation.
 *
 * @author rgabbot
 */
public class FeedbackFE extends MLFeatureExtractor {

    private String input;
    private Float[] initialValue;
    private Float[] nextValue;

    public FeedbackFE(String output, String input, Float[] initialValue, MLFeatureExtractor nextFeatureExtractor)
    {
        super(nextFeatureExtractor, output);
 
        this.input = input;
        this.initialValue = initialValue;
    }

    @Override
    public void init(double time, MLObservation.Metadata md) throws Exception {
        nextValue = initialValue;
        super.init(time, md);
    }



    @Override
    public void observe(MLObservation obs) throws Exception {

        if(nextValue != null) {
            obs.setFeature(this.getOutputName(0), nextValue);

            // this is the key... first call the rest of the chain, THEN store the value for next time.
            super.observe(obs);
        }

        Float[] newNextValue = obs.getFeatureAsFloats(input);

        if(newNextValue != null) {
            // Only feed back a non-null value.
            // (the rest of the chain might not be ready to output the feature we're feedback back, because of the "warmup period".)
            this.nextValue = newNextValue;
        }
    }
}
