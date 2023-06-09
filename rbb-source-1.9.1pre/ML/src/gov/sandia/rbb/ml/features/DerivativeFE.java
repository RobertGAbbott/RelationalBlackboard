/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;
import java.util.ArrayDeque;
import java.util.Deque;


/**
 *
 * Computes the net change (dy) in one feature (y) as another
 * feature (x) increased by a specified amount (dx).
 * x is normally time or distance.
 *
 * x must increase monotonically, although weakly (repeated x values are acceptable)
 *
 *
 * If feature X is not specified, it is assumed to be x.
 * if dx is not specified, then it is simply the change in x from the previous to the current sample.
 *
 * Examples:
 *
 * If y is distance, then dy is the rate (speed)
 *
 * If dx is 1 unit x and y is distance, then dy is the average speed over the previous unit of x.
 * Depending on the sample rate, the initial value of y may be from several samples previous.
 * 
 * If x is distance in miles and dx is 1, and y is the x, then dy is how long it took to travel
 * the previous mile (i.e. pace)
 * 
 * If dx is 1 unit x and y is altitude, then dy
 * is the climb (descent) rate over the previous unit of x.
 * 
 * If dx is 1 meter and y is altitude in meters, then dy is
 * the average slope over the previous meter.
 * 
 * Does not produce a feature or call the rest of the chain until
 * accumulating dx.  So if dx is unspecified, the derivative will be observed
 * starting with the second observation.
 *
 * @author rgabbot
 */
public class DerivativeFE extends MLFeatureExtractor {

    class Sample {
        Sample(float x, Float[] y) {
            this.x = x;
            this.y = y;
        }
        float x;
        Float[] y;
    }
    Deque<Sample> q;
    Float dx;
    String xInput, yInput;
    boolean dxAchieved; // false until we accumulate dx the first time.

    /**
     * If time is used as as one of the input features we could have a precision
     * problem.  So we offset the initialTime from observation times before
     * storing them in the Sample
     */
    Double initialTime;
    
   /**
    *
    * Compute the derivative of feature y with respect to time, over the specified interval dt.
    * If dt is null, then it is not fixed and is taken to be the time between successive Samples.
    *
    */
    public DerivativeFE(String dydtOutput, String yInput, Float dt, MLFeatureExtractor next) {
        super(next, dydtOutput);
        construct(yInput, "time", dt, next);
    }

    /**
     *
     * Compute the derivative of y with respect to x (typically distance) over the specified distance dx.
     * (x could be any cumulative value such as fuel consumed, money saved, etc).
     * <p>
     * Note: xInput must the total cumulative distance (which increases monotonically), not the distance from the previous sample to the current one.
     * If dx is null, then it is not fixed and is taken to be the difference between elapsed distance on successive samples.
     * <p>
     * To use time as yInput, specify the psuedo-input name "time."
     * This is useful to calculate the pace over a specified distance dx (the implied units are time/distance).
     *
     */
    public DerivativeFE(String dydxOutput, String yInput, String xInput, Float dx, MLFeatureExtractor next) {
        super(next, dydxOutput);
        construct(yInput, xInput, dx, next);
    }

    private void construct(String yInput, String xInput, Float dx, MLFeatureExtractor next) {
        this.xInput = xInput;
        this.yInput = yInput;
        if(yInput == null && xInput == null)
            throw new IllegalArgumentException("DerivativeFE Error: yInput and yInput cannot be both be null.");
        this.dx = dx;
        if(dx != null && dx <= 0)
            throw new IllegalArgumentException("DerivativeFE Error: dx must be null, or else > 0");
        initialTime = null;
    }

    @Override
    public void init(double time, MLObservation.Metadata md) throws Exception {
        q = new ArrayDeque<Sample>();
        dxAchieved = false;
    }

    private Float[] getFeature(MLObservation obs, String feature) {
        if(feature.equalsIgnoreCase("time")) {
            if(initialTime == null)
                initialTime = obs.getTime();
            return new Float[]{new Float(obs.getTime() - initialTime)};
        }
        else
            return obs.getFeatureAsFloats(feature);
    }

    @Override
    public void observe(MLObservation obs) throws Exception {
        Sample s = new Sample(getFeature(obs, xInput)[0], getFeature(obs, yInput));

        if(q.isEmpty()) {
            q.add(s);
            return; // there is nothing we can do with only the first observation.
        }

        // this.dx == null means just use successive samples, whatever the dx may be.
        float dx = this.dx == null ? s.x - q.getLast().x : this.dx;

        if(q.getLast().x > s.x)
            throw new Exception("Error in DerivativeFE: x must be monotonically (weakly) increasing");

        q.add(s);

        // note there is guaranteed to be a firstAfter because dx > 0.
        Sample lastBefore=null, firstAfter=null;
        while(q.getFirst().x <= q.getLast().x - dx) {
            lastBefore = q.removeFirst();
        }
        firstAfter = q.peekFirst();
        if(lastBefore != null) // put lastBefore back on... it may still be the lastBefore next x.
            q.addFirst(lastBefore);

        if(lastBefore == null) { // dx has not been reached.  Don't set the feature.
            // haven't accumulated dx yet... don't set a value.
        }
        else {
            // interpolate value for y as of dx ago.
            // dx ago is somewhere between lastBefore and firstAfter.
            if(!dxAchieved) {
                dxAchieved = true;
                super.init(obs.getTime(), obs.getMetadata());
            }
            final int dim = firstAfter.y.length;
            Float[] dydx = new Float[dim];
            final float interp = (q.getLast().x - dx - lastBefore.x) / (firstAfter.x - lastBefore.x);
            for(int i = 0; i < dim; ++i) {
                float y = lastBefore.y[i] + (firstAfter.y[i]-lastBefore.y[i]) * interp;
                dydx[i] = (q.getLast().y[i] - y) / dx; // dy/dx
            }
            obs.setFeatureAsFloats(getOutputName(0), dydx);
            super.observe(obs);
        }
    }

    @Override
    public void done(double time) throws Exception {
        if(dxAchieved)
            super.done(time);
    }

    /**
     * If x is time, the dx is how far back we need to look.
     *
     * Otherwise, can't put a limit on it.
     */
    @Override
    public Double getWarmup() {
        if(xInput == null) {
            if(dx == null)
                return 1e-6; // a tiny warmup will deliver just the previous sample, and the single previous sample is all we need if dx==null
            else
                return dx.doubleValue();
        }
        else {
            return null; // x is some other feature, like distance, and there is no telling how much time it will take to accumlate dx change in it.
        }
    }
}
