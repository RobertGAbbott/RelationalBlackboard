/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;
import gov.sandia.rbb.ml.MLObservationSequence;


/**
 *
 * Computes the derivative, i.e. change successive values per unit time.
 * The output is null for the initial observation, and if the current
 * or previous input was null.
 *
 * @author rgabbot
 */
public class RateFE extends MLFeatureExtractor {

    private MLObservationSequence data;
    private String input;

    public RateFE(String output, String input, MLFeatureExtractor next)
    {
        super(next, output);
        this.input = input;
    }

    @Override
    public void init(double time, MLObservation.Metadata md) throws Exception {
        this.data = new MLObservationSequence(3, null, md);
        super.init(time, md);
    }

    @Override
    public void observe(MLObservation obs) throws Exception {
        this.data.addObservation(obs);
        observeIfPossible();
        super.observe(obs);
        
    }

    private void observeIfPossible() {
       if(data.size() < 2)
           return;

       final MLObservation curOb = this.data.getNewest(0);
       final MLObservation prevOb = this.data.getNewest(1);

        final Float[] curX = curOb.getFeatureAsFloats(this.input);
        final Float[] prevX = prevOb.getFeatureAsFloats(this.input);

       if(curX == null || prevX == null)
           return;

        final Float[] result = new Float[curX.length];

        final float timescale = 1.0f/(float)(curOb.getTime()-prevOb.getTime());

        for(int dim = 0; dim < result.length; ++dim)
            result[dim] = timescale * (curX[dim]-prevX[dim]);

        curOb.setFeature(this.getOutputName(0), result);
    }

    /**
     * Cannot make an estimate of rate without a previous sample.
     * Returning a very small warmup value should do this; see documentation
     * on superclass getWarmup()
     */
    @Override
    public Double getWarmup() {
        return 1e-6;
    }
}




//        if(data.size()==2) { // on second observation, make an estimate for first observation.
//            prevOb.setFeature(this.getOutputName(0), result);
//            super.observe(prevOb);
//        }
