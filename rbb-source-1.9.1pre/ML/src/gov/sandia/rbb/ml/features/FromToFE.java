/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;
import gov.sandia.rbb.ml.MLObservationSequence;
import gov.sandia.rbb.Event;
import gov.sandia.rbb.impl.h2.statics.H2SEvent;
import gov.sandia.rbb.ml.RBBML.MLPart;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


/**
 *
 * FromToFE creates a timeseries for each time the
 * individual input 'mover' moves from within a threshold distance of
 * some element of the group 'from' to within a threshold distance of
 * a different element of the group 'to'.
 *
 * See the constructor documentation for more.
 *
 * @author rgabbot
 */
public class FromToFE extends MLFeatureExtractor {

    private MLObservationSequence history = null;
    private String mover, fromGroup, toGroup;
    private Float touchingThresholdDistance;
    private Double minDuration, maxDuration;
    private String from;
    private Set<String> moverEventID, moverAndFromEventIDs;

    /**
     *
     * The parameter descriptions follow the example of a gun shooting a bullet to a target.
     * There is a group of guns and a group of targets, and we want to detect
     * when a bullet moves from a gun to a target.
     *
     * @param outputFrom: the name of the output for the positions of 'from'
     * entity, e.g. "gun".  This will be present in the initial output Observation, but
     * could be null in subsequent Observations because it is drawn from the fromGroup, and group members can
     * appear and disappear during a timeseries.
     * @param outputTo: the name of the output for the positions of the 'to'
     * entity, e.g.' "targetPosition."  This will be present in the final output Observation, but
     * could be null in previous Observations because it is from a group, and group members can
     * appear and disappear during a timeseries.
     * @param mover: the name of the input for the position of the thing that is moving, e.g. 'bullet'.
     * @param fromGroup: the name of the source group, e.g. 'guns' (plural)
     * @param toGroup: the name of the destination group, e.g. 'targets' (plural)
     * @param touchingThresholdDistance: this is the longest distance at which the mover
     * is deemed to be touching a From or To.
     * @param minDuration: fromToEvents that transpire in less than this amount of
     * time are not reported (i.e. considered insignificant)
     * @param maxDuration: fromToEvents that take longer than this are not reported.
     * This bounds the amount of memory needed to buffer observations after the Mover
     * departs a From waiting to see if it will arrive at a To.
     * @param nextFE: next feature extractor in the chain.
     */
    public FromToFE(String outputFrom, String outputTo, String mover, String fromGroup, String toGroup, Float touchingThresholdDistance, Double minDuration, Double maxDuration, MLFeatureExtractor nextFE) {
        super(nextFE, outputFrom, outputTo);
        this.mover = mover;
        this.fromGroup = fromGroup;
        this.toGroup = toGroup;
        this.touchingThresholdDistance=touchingThresholdDistance;
        this.minDuration=minDuration;
        this.maxDuration=maxDuration;
    }

    @Override
    public void init(double time, MLObservation.Metadata md) throws Exception {
        history = new MLObservationSequence(null, null, md);
        moverEventID = new HashSet<String>();
        moverEventID.add(md.getFeatureEvent(mover).getID().toString());
        moverAndFromEventIDs = new HashSet<String>();
        // FromToFE will add two more inputs for its successors in the chain -
        // The from and two entities, which are promoted from group members to individuals.

        // do not call next.init() until a FromTo occurrence is found
    }

    @Override
    public void observe(MLObservation obs) throws Exception {

        if(history.size() > 0) { // we have found a start and are looking for an end
            if(maxDuration!=null && obs.getTime()-history.getOldest().getTime() > maxDuration) {
                // System.err.println("FromToFE discarding a possible event that started at "+history.getOldest().getTime()+" because it's going on for too long.");
                history.removeAll();
            }
            else {
                history.addObservation(obs);
                String[] to = NearestFE.nearest(obs, mover, null, toGroup, moverAndFromEventIDs, touchingThresholdDistance, 1);
                if(to.length > 0) { // we found an occurrence of FromTo.  Now report it.
                    if(minDuration==null || obs.getTime()-history.getOldest().getTime() >= minDuration) { // only report if long enough to be interesting.
                        // get copies of the from and to events
                        // add the from and to group inputs as individual inputs for successors.
                        Long[] fromToIDs = new Long[]{Long.parseLong(from), Long.parseLong(to[0])};
                        Event[] fromToEvents = Event.getByIDs(rbbml.getRBB(MLPart.SESSION).db(), fromToIDs);

                        MLObservation.Metadata md = history.getMetadata().clone();
                        md.setFeatureEvent(this.getOutputName(0), fromToEvents[0]);
                        md.setFeatureEvent(this.getOutputName(1), fromToEvents[1]);

                        super.init(history.getOldest().getTime(), md);
                        for(int i = 0; i < history.size(); ++i) {
                            MLObservation o = history.getOldest(i);
                            o.promoteGroupFeature(fromGroup, from, getOutputName(0));
                            o.promoteGroupFeature(toGroup, to[0], getOutputName(1));
                            super.observe(o);
                        }
                        super.done(history.getNewest().getTime());
                    }
                    history.removeAll();
                }
            }
        } 

        // allow fallthrough; the same observation may be the end of one and the start of tne next!
        // Furthermore if the same start is found again before an end is found, the history should be reset from here.

        // overwrite from with newFrom if we find a new from, but if from was already
        // valid and newFrom is null, keep from as it was.
        String[] newFrom = NearestFE.nearest(obs, mover, null, fromGroup, moverEventID, touchingThresholdDistance, 1);
        if(newFrom.length > 0) { // found a from.
//            System.err.println("FromToFE found a from: "+obs.toString());
            from=newFrom[0];

            // store the IDs of the Mover and From Event in a set so they can be excluded in the search for a To event.
            moverAndFromEventIDs.clear();
            moverAndFromEventIDs.addAll(moverEventID);
            moverAndFromEventIDs.add(from);

            // restart the history from this point.
            history.removeAll();
            history.addObservation(obs);
        }
    }

    @Override
    public void done(double time) throws Exception {
        // prevent calling next.init() until a FromTo occurrence is found
    }

    private Event[] concat(Event[] a, Event[] b) {
        Event[] result = Arrays.copyOf(a, a.length+b.length);
        for(int i = 0; i < b.length; ++i)
            result[a.length+i]=b[i];
        return result;
    }

}
