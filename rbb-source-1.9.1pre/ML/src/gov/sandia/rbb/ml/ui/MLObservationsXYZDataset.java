/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.ui;

import gov.sandia.rbb.Event;
import gov.sandia.rbb.ml.MLObservation;
import gov.sandia.rbb.ml.MLObservationSequence;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import org.jfree.data.DomainOrder;
import org.jfree.data.Range;
import org.jfree.data.general.AbstractDataset;
import org.jfree.data.xy.XYZDataset;


/**
 * <pre>
 * This is an implementation of JFreeChart XYDataset backed by
 * an array of MLObservationSequence
 *
 * Nomenclature:
 *
 * Axis: the horizontal (0) or vertical (1) axis of the plot on the screen.
 *   (todo: define axis 3 to determine the color of each data point.
 *
 * Feature: the name of the MLObservation Feature to be drawn on each axis.
 *
 * Dimension (or selectedDim): the index within the selectedFeature of the data to be drawn on each axis.
 *
 * NameTag: the value of this tag will be used as the label for each timeseries in the legend.
 *   Furthermore, all Observation Sequences with equal values for this tag are deemed "related" and will be
 *   combined into a single Series for JFreeChart, so they have the same color and
 *   collectively appear only once in the legend.  The Observation Sequences are
 *   separated by a NaN value so JFreeChart will put a break in the line.
 *
 * Example:
 * MLObservationSequence has a 2-dimensional selectedFeature named "position" whose dimensions are latitutde and longitude.
 * position selectedDim 0 (latitude) will be drawn on axis 1 (vertical)
 * position selectedDim 1 (longitude) will be drawing on axis 0 (horizontal)
 *
 * Note:
 * This class converts null feature values to Double.NaN, which is
 * what JFreeChart uses to represent values that should not be drawn.
 *
 * </pre>
 */
public class MLObservationsXYZDataset extends AbstractDataset implements XYZDataset {

    /**
     * <pre>
     * Consider seq.get(i).get(j).getOldest(k)
     * i indexes values of the nameTag, thus grouping "related" Observation sequences into a Series for JFreeChart.
     * j indexes the related sequences (all those with a single value for the nameTag)
     * k indexes the observations within a single Observation Sequence.
     * </pre>
     */
    ArrayList<ArrayList<MLObservationSequence>> seq;

    private String nameTag = null;

    /**
     * This is a reserved feature name that refers to the time of
     * observation rather than a feature in the observation.
     *
     * Like all feature names it is case sensitive.
     */
    String timeName = "Time";

    /**
     * Returns true if this dataset has a valid Z component (i.e. a feature selected for the Z axis).
     *
     */
    boolean isXYZ() { return axes.length==3; };

    /**
     * Do not modify the elements of 'seq' after passing to this object, since
     * no deep copy of seq will be made.  'seq' may be shared with other instances
     * of this class.
     * <p>
     * zFeatureName may be null; in this case, isXYZ() will return false, getZ/getZValue cannot be called, and all 'axis' parameters must be 1 or 2, not 3.
     *
     */
    public MLObservationsXYZDataset(final MLObservationSequence[] seq, String xFeatureName, String yFeatureName, String zFeatureName, String nameTag) throws SQLException {
        
        this.nameTag = nameTag;

        this.seq = new ArrayList<ArrayList<MLObservationSequence>>();
        for(MLObservationSequence seq0 : seq) {
            String name = nameTag==null ? Integer.toString(this.seq.size()) : nameTagValue(seq0);
            int related = indexOf(name);
            if(related < 0) {
                related = this.seq.size();
                this.seq.add(new ArrayList<MLObservationSequence>());
            }
            this.seq.get(related).add(seq0);
        }

        axes = new Axis[zFeatureName != null ? 3 : 2];

        // default to showing dim 0 of each axis...
        axes[0] = new Axis(xFeatureName, 0);
        axes[1] = new Axis(yFeatureName, 0);
        if(zFeatureName != null)
            axes[2] = new Axis(zFeatureName, 0);

        // ...unless x and y are the same axis.  Then use the first two dims of it.
        if(xFeatureName.equals(yFeatureName) && getFeatureDimensionality(xFeatureName) > 1)
            axes[1].selectedDim = 1;

        if(nameTag != null)
            sortByName();
    }

    /*
     * Returns the Range (min/max) of the data displayed on the specified axis.
     */
    public Range getRange(int axis) {
        Double min=null,max=null;
        Axis ax = axes[axis];
        for(ArrayList<MLObservationSequence> related : seq) {
            for(MLObservationSequence s : related) {
                if(s.size()==0)
                    continue;

                if(timeName.equals(ax.selectedFeature)) {
                    if(min==null || s.getOldest().getTime() < min)
                        min = s.getOldest().getTime();
                    continue; // time is ordered; no need to loop over the times.
                }

                for(int i = 0; i < s.size(); ++i) try {
                    Float[] f = s.getOldest(i).getFeatureAsFloats(ax.selectedFeature);
                    if(f==null)
                        continue;
                    Double x = f[ax.selectedDim].doubleValue();
                    if(min==null || x < min)
                        min = x;
                    if(max==null || x > max)
                        max = x;
                }
                catch(Exception e) {
                    // Example nonfatal exception would be attempting to get feature as floats from a non-numeric feature
                }
            }
        }

        if(min==null || max==null)
            return null;
        else
            return new org.jfree.data.Range(min, max);
    }

    static class Axis {
        /**
         * name of the selectedFeature to be drawn on this axis
         */
        String selectedFeature;

        /**
         * which dimension of the selectedFeature to be drawn on the axis (see class comment)
         */
        int selectedDim;

        Axis(String feature, int dim) {
            this.selectedFeature = feature;
            this.selectedDim = dim;
        }

        static final String[] axisNames = new String[] { "X", "Y", "Z" };
    }

    /**
     * This will have a length of 2 - axes[0] is horizontal, axes[1] is vertical.
     *
     * Todo: allow using axes[3] to determine the color of each data point.
     */
    private Axis[] axes;

    public void selectFeature(int axis, String feature, int dim) {
        axes[axis] = new Axis(feature, dim);
        fireDatasetChanged();
    }

    @Override
    public void fireDatasetChanged() {
        super.fireDatasetChanged();
    }

    public String getSelectedFeatureName(int axis) {
        return axes[axis].selectedFeature;
    }

    /**
     * Returns the dimensionality of the specified feature when interpreted
     * as a vector.
     */
    public int getFeatureDimensionality(String featureName) {
        if(seq == null)
            return 0;
        if(featureName.equals(timeName))
            return 1;
        for(ArrayList<MLObservationSequence> related : seq)
            for(MLObservationSequence s : related) {
                Integer d = s.getFeatureDimensionality(featureName);
                if(d != null)
                    return d;
        }

        return 0;
    }

    public String getAxisString(int axis) {
        final String feature = axes[axis].selectedFeature;
        if(getFeatureDimensionality(feature) > 1)
            return feature + "[" + axes[axis].selectedDim + "]";
        else
            return feature;
    }


    @Override
    public DomainOrder getDomainOrder() {
        return DomainOrder.NONE;
    }

    @Override
    public int getItemCount(int iSeries) {
        ArrayList<MLObservationSequence> as = seq.get(iSeries);
        int n = as.size()-1; // for the discontinuity between each related sequence
        for(MLObservationSequence os : as)
            n += os.size();

        return n;
    }

    @Override
    public Number getX(int i, int i1) {
        return getXValue(i,i1);
    }

    /**
     * Interpolate a value for the currently selected dimension of the currently
     * selected feature for the specified series on the specified axis at the
     * specified time.
     * <p>
     * Returns Double.NaN if the ObservationSequence had a null feature value
     * immediately before or after the designated time.
     */
    public double interpolateValue(int axis, int iSeries, double time) {
        final String featureName = axes[axis].selectedFeature;
        if(featureName == null || featureName.equals(timeName))
            return time;
        
        ArrayList<MLObservationSequence> as = seq.get(iSeries);
        for(MLObservationSequence os : as) {
            if(os.size()==0)
                continue;
            if(os.getOldest().getTime() > time)
                continue;
            if(os.getNewest().getTime() < time)
                continue;
            Float[] v = os.interpolateFeatureAsFloats(featureName, time);
            if(v!=null)
                return v[axes[axis].selectedDim];
        }
        return Double.NaN;
    }

    /**
     * Retrieve a value as a jfreechart series.
     * This is the function that combines related Observation sequences into one.
     */
    private double getValue(int axis, int iSeries, int iSample) {
        ArrayList<MLObservationSequence> as = seq.get(iSeries);
        for(MLObservationSequence os : as) {
            if(iSample == os.size())
                return Double.NaN;
            else if(iSample > os.size()) { // go to next sequence.
                --iSample; // space bwteen sequences counts as 1.
                iSample -= os.size();
                continue;
            }

            MLObservation obs = os.getOldest(iSample);

            // System.err.println("getValue " + iSeries + " " + age + " " + featureName + " " + selectedDim);
            final String featureName = axes[axis].selectedFeature;
            if(featureName == null || featureName.equals(timeName))
                return obs.getTime();

            Float[] v = obs.getFeatureAsFloats(featureName);
            if(v==null)
                return Double.NaN;

            return v[axes[axis].selectedDim];
        }

        throw new IllegalArgumentException("MLObservationsXYDataset got a bad index: iSeries="+iSeries+", iSample="+iSample);
    }

    @Override
    public double getXValue(int iSeries, int i) {
        return getValue(0, iSeries, i);
    }

    @Override
    public Number getY(int i, int i1) {
        return getYValue(i, i1);
    }

    @Override
    public double getYValue(int iSeries, int i) {
        return getValue(1, iSeries, i);
    }

    public Number getZ(int iSeries, int i) {
        return getZValue(iSeries, i);
    }

    public double getZValue(int iSeries, int i) {
        return getValue(2, iSeries, i);
    }

    @Override
    public int getSeriesCount() {
        // System.err.println("SeriesCount = " + seq.length);
        return seq.size();
    }

    @Override
    public Comparable getSeriesKey(int i) {
        return seriesName(i);
    }

    @Override
    public int indexOf(Comparable cmprbl) {
        for(int i = 0; i < seq.size(); ++i)
            if(getSeriesKey(i).equals(cmprbl))
                return i;
        return -1;
    }

    public String seriesName(int iSeries) {
        if(nameTag != null)
        try {
            return nameTagValue(seq.get(iSeries).get(0));
        } catch(SQLException e) {
        }
        
        return Integer.toString(iSeries);
    }

    private String nameTagValue(MLObservationSequence s) throws SQLException {
        return s.getMetadata().getAllFeatureEvents()[0].getTagset().getValue(nameTag);
    }

    /*
     * returns true if any of the eventIDs are inputs to the specified series.
     */
    public boolean isInput(int iSeries, Set<Long> eventIDs) {
        ArrayList<MLObservationSequence> as = seq.get(iSeries);
            for(MLObservationSequence os : as)
                for(Event ev : os.getMetadata().getAllFeatureEvents())
                    if(eventIDs.contains(ev.getID()))
                        return true;
        return false;
    }

    private void sortByName() throws SQLException {

        if(nameTag == null)
            return;

        class Compare implements Comparator<ArrayList<MLObservationSequence>> {
            boolean compareAsNumbers = true;

            Compare() throws SQLException {
                // see whether all the sequence names are parseable as numbers.
                try {
                    for(int i = 0; i < getSeriesCount(); ++i)
                        Double.parseDouble(getSeriesKey(i).toString());
                } catch(NumberFormatException e) {
                    compareAsNumbers = false;
                }

            }

            public int compare(ArrayList<MLObservationSequence> o1, ArrayList<MLObservationSequence> o2) {
                try {
                    final String n1 = nameTagValue(o1.get(0));
                    final String n2 = nameTagValue(o2.get(0));
                    if(n1 == null || n2 == null)
                        return (n1==null && n2==null) ? 0 : n1 == null ? -1 : 1;
                    if(compareAsNumbers)
                        return (int) Math.signum(Double.parseDouble(n1) - Double.parseDouble(n2));
                    else
                        return n1.compareTo(n2);
                } catch(SQLException e) {
                    return 0;
                }
            }
        }

        Collections.sort(seq, new Compare());
    }

}
