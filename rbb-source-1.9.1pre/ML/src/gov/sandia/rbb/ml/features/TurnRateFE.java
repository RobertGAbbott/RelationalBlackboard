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
 * Given a sequence of positions, computes the turn rate
 * (angular velocity) about the vertical axis.  In other words
 * this only uses the first two dimensions (x and y) and the turn rate about z is computed.
 * The result is negative when turning right and positive for left.
 *
 * No result is computed until the 3rd observation, at which time
 * the first result is repeated 3 times.
 *
 * @author rgabbot
 */
public class TurnRateFE extends MLFeatureExtractor {

    private MLObservationSequence data;
    private String input;
    int count;

    public TurnRateFE(String output, String input, MLFeatureExtractor next)
    {
        super(next, output);
        this.input = input;
    }

    @Override
    public void init(double time, MLObservation.Metadata md) throws Exception {
        this.data = new MLObservationSequence(3, null, md);
        count=0;
        super.init(time, md);
    }

    @Override
    public void observe(MLObservation obs) throws Exception {

        this.data.addObservation(obs);
        ++count;

        if(count < 3)
            return;

        final Float[] x = data.getNewest(2).getFeatureAsFloats(input);
        final Float[] y = data.getNewest(1).getFeatureAsFloats(input);
        final Float[] z = data.getNewest(0).getFeatureAsFloats(input);

        // last two 2d velocity vectors
        final Float[] a = new Float[] { y[0]-x[0], y[1]-x[1] };
        final Float[] b = new Float[] { z[0]-y[0], z[1]-y[1] };
        
        // angle = atan2(perpdot(a,b), dot(a,b));
        // = atan2( a0 * b1 - a1 * b0, a0*b0+a1*b1)
        final double theta = Math.atan2( a[0]*b[1]-a[1]*b[0], a[0]*b[0]+a[1]*b[1]);

        // convert to a rate in rad/sec
        final Float[] rate = new Float[] { (float) (theta/(data.getNewest(0).getTime()-data.getNewest(1).getTime())) };

        if(count==3) { // on second observation, make an estimate for first observation.
            data.getNewest(2).setFeature(this.getOutputName(0), rate);
            super.observe(data.getNewest(2));

            data.getNewest(1).setFeature(this.getOutputName(0), rate);
            super.observe(data.getNewest(1));
        }

        data.getNewest(0).setFeature(this.getOutputName(0), rate);
        super.observe(data.getNewest(0));
        
    }
}
