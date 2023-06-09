/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;
import gov.sandia.rbb.ml.MLObservationSequence;

import gov.sandia.cognition.statistics.distribution.UnivariateGaussian;
/**
 *
 * Calculate the weighted average of past and future observations.
 *
 * @author rgabbot
 */
public class SmoothingFE extends MLFeatureExtractor {

    private Double pastStddev, futureStddev;

    // how many standard deviations into the past or future to include.
    // 2 stddev account for 95.45 percent
    // 3 stddev account for 99.73 percent.
    final double useStddev = 3.0;

    private MLObservationSequence data;

    // indexes the oldest observation in 'data' for which no estimate has been produced.
    // this is incremented by inserting a new observation at the head of data,
    // and decremented by producing an estimate.
    // -1 if there are no predictions to make.
    private int indexOfNextEstimate;

    private String input;

   /**
     * pastStddev is the standard deviation of the gaussian kernel used to weight past observations, > 0 or null.
     * futureStddev is the standard deviation of the gaussian kernel used to weight future observations, > 0 or null.
     * At least one of pastStddev and futureStddev must be non-null.
     * If futureStddev non-null, then observations will be delayed by at least 1 sample, as this is accumulating
     * 'future' data to make an estimate for observations in the past.
     * As a SWAG, a stddev of twice the sampling frequency seems reasonable - e.g. for samples 0.05 apart, try 0.1.
     * Following this formula for futureStddev (and assuming a constant sample rate) this equates to a delay of 7 observations.
     *
     *
     */
    public SmoothingFE(String output, String input, Double pastStddev, Double futureStddev, MLFeatureExtractor next)
    {
        super(next, output);
        this.input = input;
        this.pastStddev = pastStddev;
        this.futureStddev = futureStddev;
    }

    @Override
    public void init(double time, MLObservation.Metadata md) throws Exception {

        this.data = new MLObservationSequence(null, null, md);

        this.indexOfNextEstimate=-1;

        super.init(time, md);
    }

    @Override
    public void observe(MLObservation obs) throws Exception {

        // for the moment, just store the observation so we can use it to estimate values.
        this.data.addObservation(obs);

        // since each estimate is newer than the last, each call to data.observe()
        // increments the index of the next estimate we need to make.
        ++this.indexOfNextEstimate;

//        System.err.println("times of observations in smoothing queue:");
//        for(int i = 0; i < data.size(); ++i)
//            System.err.println(data.getOldest(i).toString());

        // see if we have enough data to make some predictions, in temporal order (oldest first).
        // curTime=todo.get(todo.size()-1) is the time of the oldest prediction we need to make.
        while(this.indexOfNextEstimate >= 0 && (this.futureStddev==null || obs.getTime()-this.data.getNewest(this.indexOfNextEstimate).getTime() >= useStddev*this.futureStddev))
            doNextEstimate();

        // do NOT call nextFeatureExtractor.observe() here - it is called (only) when an estimate
        // is done, by doNextEstimate.
    }

    @Override
    public void done(double time) throws Exception {
        // finish off all the pending estimates, even if we never got as much future data as we wanted.
        while(this.indexOfNextEstimate >= 0)
            doNextEstimate();

        super.done(time);
    }

    private void doNextEstimate() throws Exception
    {
        final MLObservation curObs = this.data.getNewest(this.indexOfNextEstimate);
        final double curTime = curObs.getTime();
        Integer dim = data.getFeatureDimensionality(input);
        
        Float[] result = new Float[dim];
        for (int i = 0; i < result.length; ++i)
            result[i] = 0.0f;

        final double oldestUsefulData = curTime -(this.pastStddev==null ? 0 : useStddev * this.pastStddev);
        this.data.removeOlderThan(oldestUsefulData);

        float totalWeight = 0.0f;

        final Double pastVariance = this.pastStddev == null ? null : Math.pow(this.pastStddev, 2);
        final Double futureVariance = this.futureStddev == null ? null : Math.pow(this.futureStddev, 2);

        // add weight to samples working backwards through time, starting with the newest data.
        // Thus we might use data more than 'useStddev' after the estimate being
        // computed, but a little extra accuracy shouldn't hurt anybody.
        for(int iData=0; iData < this.data.size(); ++iData)
        {
            final Double variance = (iData < this.indexOfNextEstimate || iData==this.indexOfNextEstimate && pastVariance==null) ? futureVariance : pastVariance;
            if(variance == null)
                continue;

            final MLObservation obs = this.data.getNewest(iData);
            final Float[] x = obs.getFeatureAsFloats(this.input);

            if(x == null)
                continue;

            final float weight0 = (float) UnivariateGaussian.PDF.evaluate(obs.getTime(), curTime, variance);
            totalWeight += weight0;

            for (int iDim = 0; iDim < dim; ++iDim)
                 result[iDim] += weight0 * x[iDim];
        }

        if(totalWeight > 0) {
            final float invTotalWeight = 1.0f / totalWeight;

            // copy the result to an Object[] filled with Float and pass downstream.
            for (int i = 0; i < result.length; ++i)
                result[i] *= invTotalWeight;

            curObs.setFeature(this.getOutputName(0), result);
        }

        this.nextFeatureExtractor.observe(curObs);

        --this.indexOfNextEstimate;
    }
}
