/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;
import gov.sandia.rbb.ml.MLObservation.Metadata;
import gov.sandia.rbb.ml.MLObservationSequence;
import java.util.Arrays;


/**
 * Compute the running total (i.e. partial sum) of another feature.
 * null input values are treated as zeroes, however the output is
 * delayed until the first non-null observation because only then
 * do we know the dimension of the result.
 * The sum of each input dimension is computed independently.
 *
 * @author rgabbot
 */
public class RunningTotalFE extends MLFeatureExtractor {

    String input;
    private Float[] sum;
    MLObservationSequence initialNulls;

    public RunningTotalFE(String output, String input, MLFeatureExtractor nextFeatureExtractor) {
        super(nextFeatureExtractor, output);
        this.input = input;
    }

    @Override
    public void init(double time, Metadata md) throws Exception {
        sum=null;
        initialNulls = new MLObservationSequence(null, null, md);
        super.init(time, md);
    }

    @Override
    public void observe(MLObservation obs) throws Exception {
        Float[] a = obs.getFeatureAsFloats(input);

        if(sum == null) {
            if(a == null)
                initialNulls.addObservation(obs); // can't set the sum yet; just save for later.
            else {
//                System.err.println("RunningTotalFE: output dim = " + a.length);
                sum = new Float[a.length];
                for(int i = 0; i < a.length; ++i)
                    sum[i] = 0.0f;


                // retrospectively observe a zero vector (of the correct dimension) for initial nulls.
                for(int i = 0; i < initialNulls.size(); ++i) {
                    MLObservation old = initialNulls.getOldest(i);
                    old.setFeature(getOutputName(0), sum);
                    super.observe(old);
                }
                initialNulls.removeAll();
            }
        }

        if(sum != null) {
            for(int i = 0; i < a.length; ++i)
                sum[i] += a[i];
            obs.setFeatureAsFloats(this.getOutputName(0), sum);
            super.observe(obs);
        }

    }

    @Override
    public void done(double time) throws Exception {

        // if initialNulls is nonempty, we got nothing but nulls
        // the whole time.  But now have no choice but to
        // propagate a null running total since we don't know
        // what dimension the output would have been.
        for(int i = 0; i < initialNulls.size(); ++i)
            super.observe(initialNulls.getOldest(i));

        super.done(time);
    }

}
