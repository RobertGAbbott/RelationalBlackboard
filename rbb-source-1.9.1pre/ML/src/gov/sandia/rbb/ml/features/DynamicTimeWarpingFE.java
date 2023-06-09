/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.RBB;
import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.Timeseries;
import gov.sandia.rbb.ml.DynamicTimeWarping;
import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;
import gov.sandia.rbb.ml.MLObservation.Metadata;
import gov.sandia.rbb.ml.RBBML.MLPart;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import static gov.sandia.rbb.RBBFilter.*;

/**
 *
 * @author rgabbot
 */
public class DynamicTimeWarpingFE extends MLFeatureExtractor {
    DynamicTimeWarping dtw;
    Tagset patternFilterTags;
    double[][][] patterns;
    String[] patternNames;
    String inputFeature;
    Map<Long, Double> observationTimes;
    double warpWindow, sampleRateRatio, maxErrorRate;
    int sampleRateSteps;


    /**
     * A simplified constructor with parameters that lean towards robust matching rather than speed.
     *
     * @param inputFeature: the name of the feature to be matched against the pattern.
     * @param patternFilterTags: all timeseries matching this pattern in the model RBB will be used as queries.
     * @param next
     * @throws Exception
     */
    public DynamicTimeWarpingFE(String inputFeature, String patternFilterTags, MLFeatureExtractor next) throws Exception {
        this(inputFeature, patternFilterTags, 0.2, 2, 4, 1.0, next);
    }


    /**
     * @param inputFeature: the name of the feature to be matched against the pattern.
     * @param patternFilterTags: all timeseries matching this pattern in the model RBB will be used as queries.
     * @param warpWindow: e.g. 0.1.  In the dynamic time warping algorithm, as samples from the query are being matched to samples in the data, this is how many samples one can get ahead of the other, expressed either as a fraction of the query length (<=1.0) or a number of samples (>1)
     * @param sampleRateRatio: e.g. 2.  Because of different rates of execution or different sample rates the target in the data may consist of a different number of samples than the query.  2 means the data sample rate may be up to twice or as low as half that of the query.  1.0 disables query scaling, and sampleRateSteps is ignored.
     * @param sampleRateSteps: e.g. 5.  How many sample rates above (and also below) 1.0 to try.  For example, if sampleRateRatio were 2.0 and sampleRateSteps were 2, and the query has 100 samples, then the query will be reampled to: 50, 75, 100, 125, 150
     * @param maxErrorRate:  e.g. 1.0.  Max allowed mean positional error on the z-normed query and data.  If this is too large, you'll get "garbage" matches in between "real" ones.
     * @param next
     * @throws Exception
     */
    public DynamicTimeWarpingFE(String inputFeature, String patternFilterTags, double warpWindow, double sampleRateRatio, int sampleRateSteps, double maxErrorRate, MLFeatureExtractor next) throws Exception {
        super(next);
        this.patternFilterTags = new Tagset(patternFilterTags);
        this.inputFeature = inputFeature;
        this.warpWindow = warpWindow;
        this.sampleRateRatio = sampleRateRatio;
        this.sampleRateSteps = sampleRateSteps;
        this.maxErrorRate = maxErrorRate;
        observationTimes = new HashMap<Long,Double>();

        // this.rbbml isn't set yet so cannot get patterns from rbb yet... do it during init
    }

    @Override
    public void init(double time, Metadata md) throws Exception {
        observationTimes.clear();
        if(patterns == null) { // one-time
            // calculate the rates (query scalings) to be used.
            ArrayList<Double> sampleRates = new ArrayList<Double>();
            sampleRates.add(1.0); // always use the unscaled query.
            for(int i = 1; i <= sampleRateSteps; ++i) {
                final double r = 1+i*(sampleRateRatio-1)/sampleRateSteps;
                sampleRates.add(r);
                sampleRates.add(1/r);
            }

            RBB rbb = rbbml.getRBB(MLPart.MODEL);
            ArrayList<double[][]> patternArray = new ArrayList<double[][]>();
            ArrayList<String> patternNameArray = new ArrayList<String>();
            for(Timeseries ts : Timeseries.findWithSamples(rbb.db(), byTags(patternFilterTags))) { // for each different query pattern...
                for(Double rate : sampleRates) {
                    String name = ts.getTagset().toString() + ",DTWNumSamplesRatio="+rate;
                    patternNameArray.add(name);
                    patternArray.add(resample(ts, rate));
                    System.err.println(name + " " + patternArray.get(patternArray.size()-1).length);
                }
            }
            patterns = patternArray.toArray(new double[0][][]);
            patternNames = patternNameArray.toArray(new String[0]);
        }

        if(patterns.length == 0) {
            System.err.println("Warning: DynamicTimeWarping found no matches for the tag: "+patternFilterTags+" so no matching will be done");
            dtw = null;
            patterns = null;
        }
        else {
            System.err.println("Now create DTW");
            // note the warpWindow is rather large (25%) even though we made
            // resamplings of the query at 10% intervals above.
            // They are two different things - the resamplings allow differences in
            // total time of execution, while the warpWindow allows differens in rate
            // of execution that vary throughout.
            dtw = new DynamicTimeWarping(patterns, patternNames, warpWindow, maxErrorRate);
            System.err.println("Created DTW");
        }

        super.init(time, md);
    }

    private double[][] resample(Timeseries t, double rate) throws Exception {
        final double[][] y = new double[(int)(t.getNumSamples() * rate)][];
        final double t0 = t.getSample(0).getTime();
        final double t1 = t.getSample(t.getNumSamples()-1).getTime();
        for(int i = 0; i < y.length; ++i) {
            Float[] v = t.valueLinear(t0+(t1-t0)*i/(y.length-1));
            y[i] = new double[v.length];
            for(int k = 0; k < v.length; ++k)
                y[i][k] = v[k];
        }
        return y;
    }

    @Override
    public void observe(MLObservation obs) throws Exception {

        if(dtw != null) {
            observationTimes.put(new Long(observationTimes.size()), obs.getTime());
            Float[] v = obs.getFeatureAsFloats(inputFeature);
            double[] d = new double[v.length];
            for(int i = 0; i < v.length; ++i)
                d[i] = v[i];
            dtw.observe(d);
            report(true);
        }

        super.observe(obs);
    }

    @Override
    public void done(double time) throws Exception {
        if(dtw != null) {
            while(dtw.observe(null))
                ;
            report(false);
        }

        super.done(time);
    }

    private void report(boolean moreToCome) throws Exception {
        DynamicTimeWarping.Match m;
        while((m=dtw.getResult(moreToCome)) != null) {
            final double startTime = observationTimes.get(m.startPos());
            final double endTime = observationTimes.get(m.endPos());
            System.out.println(m+"; startTime="+startTime+", endTime="+endTime);
            RBB rbb = rbbml.getRBB(MLPart.PREDICTIONS);
            Tagset tags = new Tagset(m.queryName());
            tags.set("color", "green");
            tags.set("DTWScale", m.getScale().toString());
            final int dim = patterns[0][0].length;
            double[] dv = new double[dim];
            Float[] fv = new Float[dim];
            Timeseries ts = new Timeseries(rbb, dim, startTime, tags);
            for(int i = 0; i < m.getNumSamples(); ++i) {
                final double t = observationTimes.get(m.startPos()+i); // this is not really true; does not reflect the pairing between query samples and data samples determined by dynamic time warping algorithm.
                // System.err.println("got the " + m.startPos()+i+"th observation at t="+t);
                m.getQuerySample(i, dv);
                for(int j=0; j < dv.length; ++j)
                    fv[j] = (float) dv[j];
                ts.add(t, fv);
            }
            ts.setEnd(endTime);
            System.err.println(ts);
        }
    }
}
