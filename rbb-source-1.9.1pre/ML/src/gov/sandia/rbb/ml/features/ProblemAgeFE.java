/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;
import gov.sandia.rbb.Event;
import gov.sandia.rbb.RBB;


/**
 *
 * Output the age of the problem instance - that is, the number of units of time
 * since the problem instance was created.
 *
 * This is useful when the amount of time for which a situation persists is relevant
 * to whether it is noteworthy.
 *
 * @author rgabbot
 */
public class ProblemAgeFE extends MLFeatureExtractor {

    Double initTime;

    public ProblemAgeFE(String output, MLFeatureExtractor nextFeatureExtractor)
    {
        super(nextFeatureExtractor, output);
    }

    @Override
    public void init(double time, MLObservation.Metadata md) throws Exception {
        initTime = time;
        super.init(time, md);
    }

    @Override
    public void observe(MLObservation obs) throws Exception {
        obs.setFeatureAsFloats(this.getOutputName(0), (float)(obs.getTime()-initTime));
        super.observe(obs);
    }
}
