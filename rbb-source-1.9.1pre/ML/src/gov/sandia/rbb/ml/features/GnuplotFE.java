

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;
import gov.sandia.rbb.ml.MLObservationSequence;
import gov.sandia.rbb.Event;
import gov.sandia.rbb.RBB;
import gov.sandia.rbb.ml.MLObservation.Metadata;



/**
 * Plots the data from problem instances.
 *
 * @author rgabbot
 */
public class GnuplotFE extends MLFeatureExtractor {

    public static class Plotter
    {
        private java.util.ArrayList<MLObservationSequence> plots = new java.util.ArrayList<MLObservationSequence>();

        /**
         * Create a plot in GNUplot
         */
        public void plot(String title) throws java.io.IOException
        {
            // NOTE: if gnuplot isn't found, make sure it's in your system PATH.

            // on OSX 10.6, it appears the only way to set PATH for both the shell
            // and GUI programs (e.g. netbeans) is in /etc/launchd.conf, with a line such as:
            // setenv PATH /sw/bin:/usr/bin:/bin:/usr/sbin:/sbin:/usr/local/bin:/usr/X11/bin:/usr/X11R6/bin

            Process process = Runtime.getRuntime().exec(new String[] {"gnuplot"});
            java.io.PrintStream ps = new java.io.PrintStream(process.getOutputStream());
            plot(title, ps);
            ps.close();
            System.err.println("Sent plot to gnuplot.");
        }

        /**
         * Write the gnuplot script to the specified PrintStream.
         *
         * Specifying System.err here is an easy way to see what commands would
         * be sent to gnuplot.
         *
         */
        public void plot(String title, java.io.PrintStream ps) throws java.io.IOException
        {
            ps.println("set title '"+title+"'");
            ps.print("plot");
            java.util.HashMap<String,Integer> linestyles = new java.util.HashMap<String,Integer>();
            String delim=""; // will be set to "," after first plot
            for(MLObservationSequence plot : this.plots)
            {
                for(String feature : plot.getMetadata().getFeatureNames())
                {
                    final int dim = plot.getNewest().getFeatureAsFloats(feature).length;
                    for(int iDim = 0; iDim < dim; ++iDim)
                    {
                        // the title for each timeseries is its tagset and iDim
                        // The same Event can re-appear in multiple problem instances at different times.
                        String title0="";
                        if(dim>1) // include dimension if the timeseries has multiple dimensions
                            title0+="dim "+iDim+" of ";
                        title0 += feature;
//                        if(plot.getFeatureSet().getFeature(iFeature).ID==null)
//                            title0+=" (not in RBB)";
//                        else
//                            title0+=" EventID="+plot.getFeatureSet().getFeature(iFeature).ID;

                        final String linestyleKey = title0; // plot.getTags().toString() + ",iDim=" + iDim;
                        Integer linestyle = linestyles.get(linestyleKey);
                        if(linestyle==null) {
                            linestyle = linestyles.size()+1;
                            linestyles.put(linestyleKey, linestyle);
                        }
                      ps.print(delim+" '-' with linespoints title EventID='"+title0+"' linestyle "+linestyle);
                      delim=",";
                    }
                }
            }
            ps.println("");

            for(MLObservationSequence plot : this.plots)
            {
                for(String feature : plot.getMetadata().getFeatureNames())
                {
                    final int dim = plot.getNewest().getFeatureAsFloats(feature).length;
                    for(int iDim = 0; iDim < dim; ++iDim)
                    {
                        for(int i = 0; i < plot.size(); ++i) {
                            Float[] x = plot.getOldest(i).getFeatureAsFloats(feature);
                            ps.println(plot.getOldest(i).getTime()+" "+x[iDim]);
                        }
                        ps.println("e");
                    }
                }
            }
        }

        public void add(MLObservationSequence data)
        {
            this.plots.add(data);
        }
    }

    // instead of recording the Observations with all the features they reach us with,
    // we instead create Observations of just the feature to be plotted.
    private Metadata featureSubSet;
    private MLObservationSequence observationSequence;
    private Plotter plotter;
    private String[] plottedFeatures;

    /**
     * The 'plotter' argument to the DataCollectionEvaluator constructor may be null,
     * if so a separate plot is created for each problem instance.
     *
     * To combine multiple problem instances on one plot, instantiate a
     * Plotter object and pass it to one or more GnuplotFE instances, then after
     * evaluating the problem instances call Plotter.plot()
     *
     ***/
    public GnuplotFE(Plotter plotter, String[] plottedFeatures, MLFeatureExtractor next) {
        super(next);
        this.plottedFeatures = plottedFeatures;
        this.plotter = plotter;
        this.featureSubSet = new MLObservation.Metadata();
        for(String plottedFeature : plottedFeatures)
            this.featureSubSet.add(plottedFeature, null);
    }

    public GnuplotFE(String[] plottedFeatures, MLFeatureExtractor next) {
        super(next);
        this.plottedFeatures = plottedFeatures;
        this.plotter = null;
        this.featureSubSet = new MLObservation.Metadata();
        for(String plottedFeature : plottedFeatures)
            this.featureSubSet.add(plottedFeature, null);
    }

    @Override
    public void init(double time, MLObservation.Metadata md) throws Exception {
        this.observationSequence = new MLObservationSequence(null, null, featureSubSet);
        super.init(time, md);
    }

    @Override
    public void observe(MLObservation obs) throws Exception {
        // make a sub-observation with only the single feature.
        MLObservation subObs = new MLObservation(obs.getTime(), this.featureSubSet);
        for(String featureName : this.plottedFeatures)
            subObs.setFeature(featureName, obs.getFeature(featureName));
        this.observationSequence.addObservation(subObs);
        super.observe(obs);
    }

    @Override
    public void done(double time) throws Exception {
        if(this.plotter == null)
        {
            Plotter p = new Plotter();
            p.add(this.observationSequence);
            try {
                // make up a reasonable name for this plot
                p.plot(this.observationSequence.toString());
            } catch(java.io.IOException e)
            {
                System.err.println("MultiPlotEvaluator error: "+e.toString());
            }
        }
        else
        {
            this.plotter.add(this.observationSequence);
        }

        super.done(time);
    }

}
