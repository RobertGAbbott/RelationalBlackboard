/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml;
import gov.sandia.rbb.Event;
import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.Timeseries;
import gov.sandia.rbb.impl.h2.statics.H2STimeseries;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Evaluating a problem generates a sequence of Observations.
 * These are successive interpolated values for all the features in the model.
 * All the observations in a sequence are derived from a RBB Events.
 * All observations must reference the same MLFeatureNames instance.
 *
 * This collection is basically just an array of MLObservation instances, except:
 * 1) The sequence can be bounded by age, so old Observations are dropped as new ones are added.
 * 2) The sequence can be bounded by size, so old Observations are dropped as new ones are added.
 *
 * 
 * @author rgabbot
 */
public class MLObservationSequence {

    private Integer maxSize;
    private Double maxAge;
    MLObservation.Metadata metadata;

    /**
     * collection of observations, from oldest (0) to most recent (observations.size()-1)
     */
    ArrayList<MLObservation> observations;
    
    /**
     *
     * @param maxSize: start dropping the oldest observations if there are more than this many
     * @param maxAge: start dropping the oldest observations if it is more than this old
     * @param metadata: shared metadata for all observations in the sequence.
     */
    public MLObservationSequence(Integer maxSize, Double maxAge, MLObservation.Metadata metadata)
    {
        this.metadata=metadata;
        this.maxSize=maxSize;
        this.maxAge=maxAge;
        observations=new ArrayList<MLObservation>();
    }


    public MLObservation.Metadata getMetadata() {
        return metadata;
    }


    /**
     * Returns the dimensionality of the specified feature when interpreted
     * as a vector of floats.  Uses the first non-null value set for the feature.
     * Returns 0 if there are no non-null values set.
     */
    public int getFeatureDimensionality(String featureName) {
        for(int i = 0; i < size(); ++i)
            if(getOldest(i).getFeature(featureName) != null) {
                Object ob = getOldest(i).getFeature(featureName);
                if(ob != null) {
                    Float[] v = getOldest(i).getFeatureAsFloats(featureName);
                    if(v==null)
                        return 0;
                    return v.length;
                }
            }
        return 0;
    }


    /**
     * retrieve the i'th newest observation
     * getNewest(0) is the newest one.
     */
    public MLObservation getNewest(int age)
    {
        return this.observations.get(convertOldestNewest(age));
    }

    public MLObservation getOldest(int i)
    {
        return this.observations.get(i);
    }

    /*
     * convert from an 'oldest' index to a 'newest' index, i.e.
     * getOldest(i) = getNewest(convertOldestNewest(i)), or vice-versa.
     */
    public int convertOldestNewest(int iOldest) {
        return size()-1-iOldest;
    }

    /**
     * Preconditions: size() > 0
     */
    public MLObservation getOldest() {
        return getOldest(0);
    }

    /**
     * Preconditions: size() > 0
     */
    public MLObservation getNewest() {
        return getNewest(0);
    }

    /*
     * Get the number of observations.
     */
    public int size()
    {
        return this.observations.size();
    }

    public void addObservation(MLObservation obs)
    {
        if(size() > 0 && getNewest().getTime() >= obs.getTime())
            throw new IllegalArgumentException("ObservationSequence: Observations must be added in temporal order! "+obs+" <= "+getNewest() + " metadata: " + getMetadata());

        if(metadata == null)
            metadata = obs.metadata;

        if(size() > 0 && getOldest().metadata != obs.metadata)
            throw new RuntimeException("ObservationSequence: Observations in a sequence must reference the same Feature Set!");

        // discard if too many to make room for what we're about to add.
        if(this.maxSize != null)
            keepNewest(this.maxSize-1);

        if(this.maxAge != null)
            removeOlderThan(obs.getTime()-this.maxAge);

        this.observations.add(obs);
    }

    /**
     * Remove all observations so size()==0
     */
    public void removeAll() {
        this.observations.clear();
    }

    /*
     * remove all but the n newest observations
     */
    public void keepNewest(int n)
    {
        // ArrayList does have a removeRange operation, but it's protected.
        
        while(size() > n)
            this.observations.remove(0);
    }

    public void removeOlderThan(double time)
    {
        while(size() > 0 && getOldest().getTime() < time)
            this.observations.remove(0);
    }

    /**
     * Interpolate a value for the specified feature at the specified time,
     * by calling getFeatureAsFloats on it.
     * <p>
     * Returns null if the Observation Sequence is empty, or if either of the values
     * surrounding the specified time is null.
     */
    public Float[] interpolateFeatureAsFloats(String feature, double time) {

        if(size()==0)
            return null;
        if(size()==1)
            return observations.get(0).getFeatureAsFloats(feature);

        final int i = Collections.binarySearch(observations, new MLObservation(time, null));
        if(i >= 0) // the precise getTime was found.
            return observations.get(i).getFeatureAsFloats(feature);

        // this is the index of the last one before the specified time, unless
        // there are only two or there are none before, but in that case use the first 2 anyways.
        final int before = Math.max(0, Math.min(-i-2, observations.size()-2));
        final MLObservation a = observations.get(before);
        final MLObservation b = observations.get(before+1);

        final Float[] fa = a.getFeatureAsFloats(feature);
        final Float[] fb = b.getFeatureAsFloats(feature);

        if(fa==null || fb==null)
            return null;
        
        return H2STimeseries.interpolate(
                a.getTime(), fa,
                b.getTime(), fb,
                time);
    }
 
    /*
     * Get all the observations of the feature as a Timeseries.
     * If this feature is an Event, its tags are inherited.
     */
    public Timeseries getFeatureTimeseries(String feature) {
        Tagset tagset = new Tagset();
        Event sourceEvent = getMetadata().getFeatureEvent(feature);
        if(sourceEvent != null)
            tagset = sourceEvent.getTagset();

        Timeseries ts = new Timeseries(getOldest().getTime(), getNewest().getTime(), new Tagset(), getFeatureDimensionality(feature));
        for(int i = 0; i < observations.size(); ++i) {
            MLObservation ob = getOldest(i);
            ts.add(ob.getTime(), ob.getFeatureAsFloats(feature));
        }
        return ts;
    }

}
