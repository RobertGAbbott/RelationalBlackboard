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
 * Prints the value of one feature.
 *
 * @author rgabbot
 */
public class PrintFeatureFE extends MLFeatureExtractor {

    private String note;
    private String printFeature;

    public PrintFeatureFE(String note, String inputFeature, MLFeatureExtractor nextFeatureExtractor)
    {
        super(nextFeatureExtractor);
        this.note = note;
        this.printFeature = inputFeature;
   }

    @Override
    public void init(double time, MLObservation.Metadata md) throws Exception {

        if(this.note != null)
            System.out.print(this.note);
        System.out.print("\tinit feature " + this.printFeature + " in problem instance:");
        for(Event e : md.getAllFeatureEvents())
            System.out.print(" " + e.getID());
        System.out.println("");

        super.init(time, md);
    }

    @Override
    public void observe(MLObservation obs) throws Exception {
        if(this.note != null)
            System.out.print(this.note);
        System.out.print(obs.getTime().toString());
        Object[] x = (Object[]) obs.getFeature(this.printFeature);
        for(int i = 0; i < x.length; ++i)
            System.out.print("\t" + x[i].toString());
        System.out.println("");

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
