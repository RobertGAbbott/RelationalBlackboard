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
 * @author rgabbot
 */

/**
 *
 * Prints the entire observation using MLObservation.toString()
 *
 * @author rgabbot
 */
public class PrintObservationFE extends MLFeatureExtractor {

    private String note;

    public PrintObservationFE(String note, MLFeatureExtractor nextFeatureExtractor)
    {
        super(nextFeatureExtractor);
        this.note = note;
   }

    @Override
    public void init(double time, MLObservation.Metadata md) throws Exception {

        if(this.note != null)
            System.out.print(this.note);
        System.out.print(time);
        System.out.print("\tinit problem " + " derived from events ");
        for(Event e : md.getAllFeatureEvents())
            System.out.print(e.getID()+",");
        for(String feature : md.getFeatureNames()) {
            System.out.print(" " + feature);
            Event e = md.getFeatureEvent(feature);
            if(e!=null)
                System.out.print(" ("+e.getTagset().toString()+")");
        }
            
        System.out.println("");

        super.init(time, md);
    }

    @Override
    public void observe(MLObservation obs) throws Exception {
        if(this.note != null)
            System.out.print(this.note);
        System.out.println(obs.toString());

        super.observe(obs);
    }

    @Override
    public void done(double time) throws Exception {
         if(this.note != null)
            System.out.print(this.note);
        System.out.println(time + "\tdone");

        super.done(time);
    }


}
