/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;
import gov.sandia.rbb.ml.MLObservationSequence;
import gov.sandia.rbb.Event;
import gov.sandia.rbb.RBB;


/**
 *
 * Computes the distance traveled per unit time, with no smoothing.
 * No speed is computed until the second observation, at which time the
 * first value is repeated twice.
 *
 * @author rgabbot
 */
public class SpeedFE extends MLFeatureExtractor {

    private MLObservationSequence data;
    private String input;

    public SpeedFE(String output, String input, MLFeatureExtractor next)
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

        if(data.size() == 1)
            return;

        final MLObservation curOb = this.data.getNewest(0);
        final MLObservation prevOb = this.data.getNewest(1);
        
        final Float[] curX = curOb.getFeatureAsFloats(this.input);
        final Float[] prevX = prevOb.getFeatureAsFloats(this.input);
        final double dt = curOb.getTime()-prevOb.getTime();
        final float speed = (float)(DistanceFE.distance(curX, prevX) / dt);

        if(data.size()==2) { // on second observation, make an estimate for first observation.
            prevOb.setFeatureAsFloats(this.getOutputName(0), speed);
            super.observe(prevOb);
        }

        obs.setFeatureAsFloats(this.getOutputName(0), speed);
        super.observe(obs);
        
    }
}
