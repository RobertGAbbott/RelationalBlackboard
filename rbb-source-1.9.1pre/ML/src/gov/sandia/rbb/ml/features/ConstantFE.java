/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;
import gov.sandia.rbb.RBB;


/**
 *
 * Output a constant value.
 *
 *
 * @author rgabbot
 */
public class ConstantFE extends MLFeatureExtractor {

    String input;
    private Float[] d;


    /**
     * Applies a constant offset, d, to the input.
     * Input may be null, in which case d itself is output.
     */
    public ConstantFE(String output, String input, Float[] d, MLFeatureExtractor nextFeatureExtractor)
    {
        super(nextFeatureExtractor, output);
        this.d=d;
        this.input=input;
    }

    @Override
    public void observe(MLObservation obs) throws Exception {
        if(input == null)
            obs.setFeatureAsFloats(this.getOutputName(0), d);
        else {
            Float[] x = obs.getFeatureAsFloats(input);
            Float[] y = new Float[x.length];
            for(int i = 0; i < d.length; ++i)
                y[i] = x[i] + d[i];
            obs.setFeature(this.getOutputName(0), y);
        }
        super.observe(obs);
    }
}
