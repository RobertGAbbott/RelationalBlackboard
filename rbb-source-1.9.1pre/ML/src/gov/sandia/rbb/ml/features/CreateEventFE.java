/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.Event;
import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.impl.h2.statics.H2SEvent;

import gov.sandia.rbb.ml.MLObservation;
import gov.sandia.rbb.ml.RBBML.MLPart;


/**
 * Create a new RBB Event (discarding any timeseries data) for each problem instance.
 *
 * Does not create a Feature in the Observation
 */
public class CreateEventFE extends MLFeatureExtractor {

    private Event event;
    private Tagset tags, resultTags;
    private Boolean print, store;
    private Double startTime;


    /**
     * 
     * The 'tags' parameter specifies the tagset for any events created by this feature extractor.
     * <p>
     * Tags from the inputs can also be inherited using the syntax:
     * <pre>
     * tagname=inputname.inputTagname
     * </pre>
     * Or, to inherit all the parent tags except for one with a new value:
     * <pre>
     * *=inputName.*,myTag=newValue
     * </pre>
     *
     * For more detail see <a href="../MLObservation.Metadata.html#tagValueSubstitution(Tagset)">MLObservation.Metadata.tagValueSubstitution()</a>
     *
     */
    public CreateEventFE(Tagset tags, Boolean print, Boolean store, MLFeatureExtractor next)
    {
        super(next);
        this.tags = tags;
        this.print = print;
        this.store = store;
    }

    @Override
    public void init(double time, MLObservation.Metadata md) throws Exception {

        resultTags = md.tagValueSubstitution(tags);
        startTime = time;

        if(store)
            this.event = new Event(rbbml.getRBB(MLPart.PREDICTIONS).db(), time, time, resultTags);

        // System.err.println("CreateEventFE created event " + this.eventID + " with tags " + tags.toString() + " at t="+time);
        
        super.init(time, md);
    }

    @Override
    public void done(double time) throws Exception {
        if(store)
            H2SEvent.setEndByID(rbbml.getRBB(MLPart.PREDICTIONS).db(), event.getID(), time);
        if(print) {
            if(time == startTime)
                System.out.print("time="+time);
            else
                System.out.print("start="+startTime+",end="+time+",");
            System.out.println(resultTags.toString());
        }


        super.done(time);
    }

}
