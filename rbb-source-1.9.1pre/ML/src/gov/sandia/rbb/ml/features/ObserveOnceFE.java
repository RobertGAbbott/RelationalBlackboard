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
 * Passes observation down the chain only once for each problem instance -
 * the first observation at or after the specified time.
 *
 * @author rgabbot
 */
public class ObserveOnceFE extends MLFeatureExtractor {

    private Double goTime;
    private boolean done = false;
    private Event[] inputs;

    public ObserveOnceFE(Double goTime, MLFeatureExtractor nextFeatureExtractor)
    {
        super(nextFeatureExtractor);
        this.goTime = goTime;
    }


    @Override
    public void init(double time, MLObservation.Metadata md) throws Exception {
        done = false;
        this.inputs = inputs;

        //// do not call super.init at this time.
        //// the flagCreator is not started until a flag is found.
    }

    @Override
    public void observe(MLObservation obs) throws Exception {
       if(!done && obs.getTime() >= this.goTime)
        {
            if(this.nextFeatureExtractor != null)
            {
                this.nextFeatureExtractor.init(obs.getTime(), obs.getMetadata());
                this.nextFeatureExtractor.observe(obs);
                this.nextFeatureExtractor.done(obs.getTime());
            }
            done = true;
        }
        // do not call super.observe()
    }

    @Override
    public void done(double time) throws Exception {
        // do not call super.done()
    }
}
