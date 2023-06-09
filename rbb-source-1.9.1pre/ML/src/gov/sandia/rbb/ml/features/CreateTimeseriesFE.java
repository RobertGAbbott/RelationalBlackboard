/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;
import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.Timeseries;
import gov.sandia.rbb.ml.RBBML.MLPart;
import gov.sandia.rbb.util.StringsWriter;


/**
 * Stores the feature as an RBB Timeseries.
 * Null-valued observations are ignored (not stored)
 * If no observation of the timeseries is ever received, then it won't be created after all.
 *
 */
public class CreateTimeseriesFE extends MLFeatureExtractor {

    protected Double startTime;
    protected Timeseries score;
    protected Tagset tags; // tags for all new timeseries.
    protected String input;
    protected Boolean print, store;
    protected Boolean started;


    /**
     *
     * The 'tags' parameter specifies the tagset for any Timeseries created by this feature extractor.
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
     * No samples are added to the timeseries if/when the input is null.
     *
     */
    public CreateTimeseriesFE(Tagset tags, String input, Boolean print, Boolean store, MLFeatureExtractor next)
    {
        super(next);
        this.input = input;
        this.tags = tags;
        this.print = print;
        this.store = store;
    }

    @Override
    public void init(double time, MLObservation.Metadata md) throws Exception {
        startTime = time;
        score = null;
        started = false;
        super.init(time, md);
    }

    @Override
    public void observe(MLObservation obs) throws Exception {

        Float[] x = obs.getFeatureAsFloats(this.input);

        if(x==null) {
            super.observe(obs);
            return;
        }

        if(started == false) {
            Tagset resultTags = obs.getMetadata().tagValueSubstitution(tags);
            // first observation received.
            // Creation is deferred until now because until this time we don't know what the dimension of the observed data will be.
            if(store)
                score = new Timeseries(rbbml.getRBB(MLPart.PREDICTIONS), x.length, startTime, resultTags);

            if(print)
                System.out.println(resultTags.toString());

            started = true;
        }

        if(score != null)
            score.add(rbbml.getRBB(MLPart.PREDICTIONS), obs.getTime(), x);

        if(print)
            System.out.println(obs.getTime()+","+StringsWriter.join(",", obs.getFeatureAsFloats(input)));

        super.observe(obs);
    }

    @Override
    public void done(double time) throws Exception {
        if(score != null)
            score.setEnd(rbbml.getRBB(MLPart.PREDICTIONS).db(), time);
        score = null;
        
        if(print && started)
            System.out.println("");

        super.done(time);
    }
}
