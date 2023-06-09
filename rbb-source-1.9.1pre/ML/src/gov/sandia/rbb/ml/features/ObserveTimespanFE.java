/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;


/**
 * Passes observation down the chain only between the specified start and end times.
 *
 * as a special case, the first observation after the start time
 * is used, even if it is after the end time!
 * This way start==end can be used to create exactly one observation.
 * Without this special case it would be very easy to wind up without
 * any observations at all, if end-start > timestep
 * 
 *
 * @author rgabbot
 */
public class ObserveTimespanFE extends MLFeatureExtractor {

    private Double start, end;

    private enum State { Not_Initialized, Initialized, Started, Ended };

    private State state;

    public ObserveTimespanFE(Double start, Double end, MLFeatureExtractor nextFeatureExtractor)
    {
        super(nextFeatureExtractor);
        this.start = start;
        this.end = end;
        this.state = State.Not_Initialized;
    }


    @Override
    public void init(double time, MLObservation.Metadata md) throws Exception {
        this.state = State.Initialized;

        //// do not call super.init at this time.
        //// the flagCreator is not started until a flag is found.
    }

    @Override
    public void observe(MLObservation obs) throws Exception {

        if(this.nextFeatureExtractor == null)
            return;

        switch (this.state) {
            case Not_Initialized:
                break; // this shouldn't happen, so it could throw an exception here.

            case Initialized:
                if(this.start == null || obs.getTime() >= this.start) {

                    this.nextFeatureExtractor.init(obs.getTime(), obs.getMetadata());
                    this.state = State.Started;
                    // as a special case, the first observation after the start time
                    // is used, even if it is after the end time!
                    // This way start==end can be used to create exactly one observation.
                    // Without this special case it would be very easy to wind up without
                    // any observations at all, if end-start > timestep
                    this.nextFeatureExtractor.observe(obs);
                    if(this.end != null  && obs.getTime() > this.end)
                        done(obs.getTime());
                }
                break;

            case Started:
                if(this.end != null && obs.getTime() > this.end)
                    done(obs.getTime());
                else
                    this.nextFeatureExtractor.observe(obs);
                break;

            case Ended:
                break;
        }

        // do not call super.observe()
    }

    @Override
    public void done(double time) throws Exception {
        // do not call super.done()

        if(this.state == State.Started) {
            this.nextFeatureExtractor.done(time);
            this.state = State.Ended;
        }
    }
}
