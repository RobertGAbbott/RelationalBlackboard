/*
 * File:                Timeseries.java
 * Authors:             Robert G. Abbott
 * Company:             Sandia National Laboratories
 * Project:             Relational Blackboard Core
 *
 * Copyright September 16, 2010, Sandia Corporation.
 * Under the terms of Contract DE-AC04-94AL85000, there is a non-exclusive
 * license for use of this work by or on behalf of the U.S. Government. Export
 * of this program may require a license from the United States Government.
 */
package gov.sandia.rbb;

import gov.sandia.rbb.impl.h2.statics.H2SRBB;
import java.util.Map;
import java.util.HashMap;
import java.sql.Connection;
import gov.sandia.rbb.impl.h2.statics.H2STimeseries;
import gov.sandia.rbb.util.StringsWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import static gov.sandia.rbb.RBBFilter.*;

/**
 *<pre>
 * A memory-resident copy of a timeseries.
 *
 * This class does not keep itself in sync with the contents of the RBB automatically;
 * for that, see TimeseriesCache.
 *
 * Persistent vs. in-RAM changes:
 * Any method that persistently changes the Event in the RBB requires an RBB parameter.
 * Any method that doesn't take an RBB parameter, does not persistently change the Event in the RBB.
 *</pre>
 * @author rgabbot
 */
public class Timeseries extends Event
{
    ArrayList<Sample> samples;
    int dim;

    final Integer interpolateLinear = 1;
    final Integer interpolatePrev = 2;
    Integer interpolate;

    /*
     * Create a new Timeseries persistently (in the RBB)
     */
    public Timeseries(RBB rbb, int dim, double start, Tagset tags) throws SQLException
    {
        this(H2STimeseries.start(rbb.db(), dim, start, tags.toString()), start, H2SRBB.maxDouble(), tags, dim);
        samples = new ArrayList<Sample>();
        isPersistent = true;
    }

    /*
     * Construct a persistent instance from pre-existing data.
     * i.e. it is assumed id is an actual RBB ID,
     * but will not have any samples unless / unless loadSamples is called.
     */
    public Timeseries(Long id, Double start, Double end, Tagset tagset, int dim) {
        super(id,start,end,tagset);
        this.dim = dim;
        samples = new ArrayList<Sample>();
        isPersistent = true;
    }

    /*
     * Construct a transient instance with no samples.
     */
    public Timeseries(Double start, Double end, Tagset tagset, int dim) {
        super(start,end,tagset);
        this.dim = dim;
        samples = new ArrayList<Sample>();
        isPersistent = false;
    }

    /*
     * Promote an Event whose ID has timeseries data in the RBB to a Timeseries.
     */
    public Timeseries(Event e, int dim) {
        this(e.getID(), e.start, e.end, e.getTagset(), dim); // access e.start/e.end directly to avoid time conversion.  Get them literally, and the timeConverter separately.
        timeConverter = e.timeConverter;
    }

    /*
     * Access the Timeseries with a different time coordinate.
     * f is created with RBBFilter.usingTimeCoordinate()
     * This new instance will share samples with the Timeseries it is copied from.
     * Even though this constructor takes a DB connection, it does NOT create a new
     * persisten Timeseries; the connection is only to look up the time conversion parameters.
     */
    public Timeseries(Timeseries e, RBBFilter f, Connection conn) throws SQLException {
        super(e,f,conn);
        this.dim = e.dim;
        this.samples = e.samples; // no deep copy
    }

    /**
     * get from rbb by ID.
     * Throws IllegalArgumentException for invalid ID.
     *<p>
     * If one of the (optional) filters is withTimeCoordinate, then the times (start/end) are converted in the result.
     * If the Timeseries specified by the ID doesn't match any of the filters, it won't be found and the exception will be thrown.
     *
     */
    public static Timeseries getByIDWithoutSamples(Connection conn, long id, RBBFilter... f) throws SQLException, IllegalArgumentException {
        Timeseries[] t = findWithoutSamples(conn, byID(id), new RBBFilter(f));
        if(t.length == 0)
            throw new IllegalArgumentException("Timeseries.getByIDWithoutSamples: invalid Event ID "+id);
        return t[0];
    }

    /*
     * Returns a copy of events with Event intances replaced by Timeseries instances
     * for each element of events that has Timeseries data.  The caller may use
     * instanceof to determine which are timeseries.  
     *<p>
     * Any Timeseries instances already in events are also simply copied into the result and not cloned.
     *<p>
     * The returned Timeseries will not
     * have any samples; call e.g. getSamples on individual timeseries to get the samples.
     *<p>
     * See TimeseriesTest.testPromoteTimeseries for examples.
     */
    public static Event[] promoteTimeseries(Connection conn, Event[] events) throws SQLException {
        synchronized(conn) {
            Event[] result = new Event[events.length];
            ArrayList<Long> eventIDs = new ArrayList<Long>();
            for(Event e : events)
                if(!(e instanceof Timeseries))
                    eventIDs.add(e.getID());
            ResultSet rs = H2STimeseries.getDims(conn, eventIDs.toArray());
            Map<Long,Integer> dims = new HashMap<Long, Integer>();
            while(rs.next())
                dims.put(rs.getLong(1), rs.getInt(2));
            for(int i = 0; i < events.length; ++i) {
                Integer dim = dims.get(events[i].getID());
                if(dim == null)
                    result[i] = events[i]; // no promotion.
                else
                    result[i] = new Timeseries(events[i],dim);
            }
            rs.close();
            return result;
        }
    }

   /*
    * Populate the Timeseries Samples with all the samples from the RBB.
    * Any Samples previously in this timeseries are replaced.
    */
   public void loadAllSamples(Connection conn) throws SQLException {
       ResultSet rs = H2STimeseries.getSamples(conn, getID(), null, null, 0, 0, null, null);
       samples.clear();
       while(rs.next())
            samples.add(new Sample(rs)); // do not use the public add() method because it would do time conversion.
   }

  /*
    * Populate the Timeseries Samples with the N most recent samples from the RBB.
    * Any Samples previously in this timeseries are replaced.
    */
    public void loadRecentSamples(Connection conn, int n) throws SQLException {
        samples.clear();
        if(n==0)
            return;
        ResultSet rs = H2STimeseries.getSamples(conn, getID(), Double.MAX_VALUE, Double.MAX_VALUE, n, 0, null, null);
        while(rs.next())
            samples.add(new Sample(rs)); // do not use the public add() method because it would do time conversion.
   }

    @Override
    public String toString() {
        return super.toString() + ", numSamples=" + getNumSamples();
    }

    public int getDim() {
        return dim;
    }

    /*
     * Get the number of samples loaded or added to this Timeseries instance.
     * Note in the case of a persistent Timeseries this is not necessarily the same as the number in the RBB
     * (H2STimeseries.getNumSamples())
     */
    public int getNumSamples() {
        return this.samples.size();
    }

    /*
     * Add a Sample this instance, nonpersistently.
     */
    public void add(double time, Float... data) {
        if(isPersistent != null && isPersistent) // see comment on declaration of isPersistent
            throw new IllegalArgumentException("Timeseries.add: the instance was created persistently but was then called without an RBB parameter");
        add(new Sample(time, data));
    }


    /*
     * Add a Sample this instance and the rbb.
     */
    public void add(RBB rbb, double time, Float... data) throws SQLException {
        if(isPersistent != null && !isPersistent) // see comment on declaration of isPersistent
            throw new IllegalArgumentException("Timeseries.add: the instance was created transiently but was then called with an RBB parameter");
        add(new Sample(time, data));
        Sample s = samples.get(samples.size()-1);
        H2STimeseries.addSampleByID(rbb.db(), id, unMapTime(time), data, null, null);
    }


    /*
     * Add a Sample this instance nonpersistently.
     *<p>
     * The other overloads of add() all go through this one, so that if it is
     * overridden (e.g. TimeseriesTimeConverter), just this one needs to be overridden.
     */
    public void add(Sample s) {
        if(timeConverter != null)
            s = new Sample(unMapTime(s.time), s.value);
        addWithoutTimeConversion(s);
    }

    /**
     * Add a sample without doing time conversion, for example TimeseriesCache
     * calls this when it receives a new sample from the RBB.
     */
    void addWithoutTimeConversion(Sample s) {
        if(this.samples.size() > 0 &&
            s.getTime() <= samples.get(samples.size()-1).getTime())
           throw new IllegalArgumentException("TimeseriesCopy.add error - data added out of time order to timeseries " + this.getTagset() + "; got t="+s.getTime()+", already had "+samples.get(samples.size()-1).getTime());
        samples.add(s);
    }

    /*
     * Use linear interpolation between the two values surrounding the specified
     * time to estimate a value for the timeseries.
     * <p>
     * Returns null if the specified time is outside the start/end of the Event.
     */
    public Float[] valueLinear(double time) {
        if(time < getStart() || time > getEnd())
            return null;    
        return extrapolateValueLinear(time);
    }
    
    /*
     * extrapolateValueLinear is the same as valueLinear, except it will produce
     * an estimate even outside the start/end time of the Event by extrapolating
     * from the nearest values.  (If the time is between two samples, then 
     * interpolation, not extrapolation, is still used).
     */
    public Float[] extrapolateValueLinear(double time) {
        time = unMapTime(time);
        // bad parameters.
        if(this.getNumSamples()==0)
            throw new IllegalArgumentException("Timeseries.extrapolateValueLinear error - called on empty timeseries " + this.getID());

        // trivial cases
        if(this.getNumSamples()==1)
            return this.samples.get(samples.size()-1).getValue();
        if(this.getNumSamples()==2)
            return valueLinear(time, 0, 1);

        final int i = Collections.binarySearch(samples, new Sample(time, null));
        if(i >= 0) // the precise getTime was found.
            return this.samples.get(i).getValue();
        final int before = Math.max(0, Math.min(-i-2, samples.size()-2));
        return valueLinear(time, before, before+1);
    }
    
    public Float[] valuePrev(double time) {
        time = unMapTime(time);
        if(this.getNumSamples()==0)
            throw new IllegalArgumentException("Timeseries.valuePrev error - called on empty timeseries " + this.getID());
        if(this.getNumSamples()==1)
            return this.samples.get(0).getValue();
        final int i = Collections.binarySearch(samples, new Sample(time, null));
        if(i >= 0) // the precise getTime was found.
            return this.samples.get(i).getValue();
        final int before = Math.max(0, -i-2);
        return samples.get(before).getValue();
    }

    public Float[] value(double time) {
        if (interpolate == null)
            interpolate = getInterpolateFromTags();

        if(interpolate == interpolateLinear)
            return valueLinear(time);
        else
            return valuePrev(time);
    }

    private Integer getInterpolateFromTags() {
        final String interpolateString = getTagset().getValue("interpolate");

        if (interpolateString == null || interpolateString.equals("linear"))
            return interpolateLinear;
        else if (interpolateString.equals("prev"))
            return interpolatePrev;
        else
             throw new IllegalArgumentException("Timeseries: tried to apply unknown interpolation type " + interpolateString);
    }

    private Float[] valueLinear(double t, int i1, int i2) {
        return samples.get(i1).interpolateValue(samples.get(i2), t);
    }

    public static class Sample implements Comparable<Sample> {

        Float[] value;
        Double time;

        public Sample(Double time, Float[] value) {
            this.value = value;
            this.time = time;
        }

        public Sample(double t,
            Sample a,
            Sample b)
        { // make a new row interpoalted / extrapolated from two others.
            time = t;
            value = H2STimeseries.interpolate(a.getTime(), a.getValue(), b.getTime(), b.getValue(), t);
        }

        public Sample(ResultSet rs) throws SQLException
        {
            time = rs.getDouble(1);
            Object[] a = (Object[]) rs.getArray(2).getArray();
            value = new Float[a.length];
            for (int i = 0; i < a.length; ++i) {
                if(a[i] instanceof Float)
                    value[i] = (Float) a[i];
                else
                    value[i] = Float.parseFloat(a[i].toString());
            }
        }

        public Float[] getValue() { return value; };

        public Double getTime() { return time; };

        public void setTime(Double time) {
            this.time = time;
        }

        /**
         * Compare by time.
         */
        @Override
        public int compareTo(Sample b) {
            return time.compareTo(b.getTime());
        }

        Float[] interpolateValue(Sample b, double newTime) {
            return H2STimeseries.interpolate(
                time, value,
                b.getTime(), b.getValue(),
                newTime);
        }

        @Override public String toString() {
            return "Sample(t="+time+", x="+StringsWriter.join(",",value)+")";
        }

        @Override public boolean equals(Object o) {
            if(!(o instanceof Sample)) return false;
            Sample s = (Sample) o;
            return s!=null && time.equals(s.getTime()) && Arrays.deepEquals(value, s.getValue());
        }
    }

    /*
     * Override to drop Samples now out of the time range.
    */
    @Override
    protected void setInstanceTimes(Double startTime, Double endTime) {
        super.setInstanceTimes(startTime, endTime);

        // start time
        int dropOldest = 0;
        while(dropOldest < getNumSamples() && samples.get(dropOldest).time < start)
            ++dropOldest;
        if(dropOldest > 0)
            keepNewest(getNumSamples()-dropOldest);

        int dropNewest = 0;
        while(dropNewest < getNumSamples() && samples.get(samples.size()-1-dropNewest).time > end)
            ++dropNewest;
        if(dropNewest > 0)
            samples.subList(samples.size()-dropNewest, samples.size()).clear();
    }


    /*
     * Discard all but the n last values.
     * This affects only the Samples in this copy (even if it is a persistent copy)
     *<p>
     * I see no efficient way to implement this directly with java.util.* because nothing in
     * java.util implements both the Queue interface, and the List interface which is necessary for binarySearch
     */
    public void keepNewest(int n) {
        if(samples.size() > n)
            samples.subList(0, samples.size()-n).clear();
    }

    public Sample getSample(int i) {
        if(timeConverter == null)
            return samples.get(i);
        else
            return new Sample(mapTime(samples.get(i).getTime()), samples.get(i).getValue());
    }

    public Sample[] getSamples() {
        Sample[] result = new Sample[getNumSamples()];
        for(int i = 0; i < getNumSamples(); ++i)
            result[i] = getSample(i);
        return result;
    }

    /*
     * Get all samples in the specified inclusive time range
     */
    public Sample[] getSamples(double t0, double t1) {
        t0 = unMapTime(t0);
        t1 = unMapTime(t1);
        // bad parameters.
        if(this.getNumSamples()==0)
            throw new IllegalArgumentException("Timeseries.getSamples error - called on empty timeseries " + this.getID());

        // i0 will index the first sample at or after t0
        int i0 = Collections.binarySearch(samples, new Sample(t0, null));
        if(i0 < 0) // the exact time was not found, so use the first after.
            i0 = -i0 - 1;

        // t1 will index the first sample after t1
        int i1 = Collections.binarySearch(samples, new Sample(t1, null));
        if(i1 >= 0) // t1 was found exactly, so increment by 1 so indexes first after.
            ++i1;
        else // the exact time was not found, so use the first after.
            i1 = -i1 - 1;

        final int n = i1-i0;

        if(n <= 0)
            return new Sample[0];

        Sample[] result = new Sample[i1-i0];
        for(int i = 0; i < n; ++i)
            result[i] = getSample(i+i0);

        return result;

    }

    /*
     * Retrieve a sample without performing time conversion.
     * Mainly this is for internal use by the implementation
     */
    Sample getSampleWithoutTimeConversion(int i) {
        return samples.get(i);
    }


    /*
     * Retrieve timeseries, but don't get any samples.
     * <p>
     * If one of the filters is withTimeCoordinate, then the times (start/end) are converted in the result.
     */
    public static Timeseries[] findWithoutSamples(Connection conn, RBBFilter... f) throws SQLException {
        RBBFilter filter = new RBBFilter(f);
        filter.also(bySchema(H2STimeseries.schemaName));
        ArrayList<Timeseries> result = new ArrayList<Timeseries>();
        for(Event ev : Event.find(conn, filter)) {
            Timeseries ts = new Timeseries(ev, H2STimeseries.getDim(conn, ev.getID()));
            ts.isPersistent = true;
            result.add(ts);
        }
        return result.toArray(new Timeseries[0]);
    }

    /*
     * Retrieve timeseries with any/all Samples.
     * <p>
     * If one of the filters is withTimeCoordinate, then the times (start/end/sample times) are converted in the result.
     */
    public static Timeseries[] findWithSamples(Connection conn, RBBFilter...f) throws SQLException {
        Timeseries[] timeseries = findWithoutSamples(conn, f);
        for(Timeseries ts : timeseries)
            ts.loadAllSamples(conn);
        return timeseries;
    }

    /*
     * Find timeseries with the N most recent (latest) Samples
     * <p>
     * If one of the filters is withTimeCoordinate, then the times (start/end/sample times) are converted in the result.
     */
    public static Timeseries[] findWithRecentSamples(Connection conn, Integer n, RBBFilter...f) throws SQLException {
        RBBFilter filter = new RBBFilter(f);
        Timeseries[] results = findWithoutSamples(conn, filter);
        if(n != null && n == 0)
            return results;
        for(Timeseries ts : results) {
            if(n == null) // get all
                ts.loadAllSamples(conn);
            else
                ts.loadRecentSamples(conn, n);
        }
        return results;
    }


    /*
     * Store this transient Timeseries in the RBB.
     * The previous ID is overwritten with a new one.
     */
    public void persist(Connection conn) throws SQLException {
        if(isPersistent != null && isPersistent) // see comment on declaration of isPersistent
            throw new IllegalArgumentException("Timeseries.persistent: called on an instance that was already persistent.");

        ArrayList<Double> times = new ArrayList<Double>();
        ArrayList<Float[]> data = new ArrayList<Float[]>();

        for(Sample s : samples) {
            times.add(s.getTime());
            data.add(s.getValue());
        }

        id = H2STimeseries.create(conn, dim, start, end, tagset.toString(), times.toArray(), data.toArray());

        isPersistent = true;
    }

//
//    /*
//     * Store this transient Timeseries in the RBB, unless an existing Timeseries already had an identical tagset.
//     * In that case, re-initialize this instance with the contents of that timeseries, including the samples.
//     *<p>
//     * If multiple timeseries already had that tagset, it is indeterminate which will be used.
//     */
//    public void persistUnique(Connection conn) throws SQLException {
//        if(isPersistent != null && isPersistent) // see comment on declaration of isPersistent
//            throw new IllegalArgumentException("Timeseries.persistentUnique: called on an instance that was already persistent.");
//
//        Timeseries[] prev = findWithoutSamples(conn, byTags(getTagset()));
//        if(prev.length > 0)
//            setEqual(prev);
//        else
//            persist(conn);
//    }

}
