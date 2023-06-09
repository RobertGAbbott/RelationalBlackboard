package gov.sandia.rbb.ui.chart;

import gov.sandia.rbb.RBB;
import gov.sandia.rbb.RBBFilter;
import gov.sandia.rbb.Timeseries;
import gov.sandia.rbb.EventCache;
import java.sql.SQLException;
import org.jfree.data.DomainOrder;
import org.jfree.data.general.AbstractDataset;
import org.jfree.data.xy.XYDataset;

/**
 * This is an implementation of JFreeChart XYDataset backed by an RBB timeseries.
 * The X value is time.
 *
 * Todo: call fireDatasetChanged when timeseries modified.
 */
class TimeseriesXYDataset extends AbstractDataset implements XYDataset {

    /**
     * Call destroy() on this instance when no longer needed, since it listens to the RBB for changes to stay up-to-date.
     */
    public TimeseriesXYDataset(RBB rbb, RBBFilter... filter) throws SQLException {
        ts = new EventCache(rbb);
        ts.initCache(filter);
    }

    public void destroy() throws SQLException {
        ts.disconnect();
    }

    EventCache ts;

    @Override
    public DomainOrder getDomainOrder() {
        return DomainOrder.ASCENDING;
    }

    @Override
    public int getItemCount(int i) {
        try {
            return ts.findTimeseries()[i].getNumSamples();
        } catch (SQLException ex) {
            System.err.println("Plot error: "+ex.toString());
            return 0;
        }
    }

    @Override
    public Number getX(int i, int i1) {
        return getXValue(i,i1);
    }

    @Override
    public double getXValue(int i, int i1) {
        try {
            return ts.findTimeseries()[i].getSample(i1).getTime();
        } catch (SQLException ex) {
            System.err.println("Plot error: "+ex.toString());
            return 0;
        }
    }

    @Override
    public Number getY(int i, int i1) {
        return getYValue(i, i1);
    }

    @Override
    public double getYValue(int i, int i1) {
        try {
            return ts.findTimeseries()[i].getSample(i1).getValue()[0];
        } catch (SQLException ex) {
            System.err.println("Plot error: "+ex.toString());
            return 0;
        }
    }

    @Override
    public int getSeriesCount() {
        try {
            return ts.findTimeseries().length;
        } catch (SQLException ex) {
            System.err.println("Plot error: "+ex.toString());
            return 0;
        }
    }

    @Override
    public Comparable getSeriesKey(int i) {
        try {
            Timeseries tsi = ts.findTimeseries()[i];
            return tsi.getID();
        } catch (SQLException ex) {
            System.err.println("Plot error: "+ex.toString());
            return 0;
        }
    }

    /**
     * Get the index of a timeseries by its RBBID.
     * Throws IllegalArgumentException if not found.
     */
    @Override
    public int indexOf(Comparable cmprbl) {
        try {
        Timeseries[] t = ts.findTimeseries();
        for(int i = 0; i < t.length; ++i)
            if(t[i].getID().equals(cmprbl))
                return i;
        } catch (SQLException ex) {
            System.err.println("Plot error: "+ex.toString());
        }
        throw new IllegalArgumentException("TimeseriesXYDataset got invalid paramater to indexOf: "+cmprbl.toString());
    }

    void setFilter(RBBFilter... filter) throws SQLException {
        ts.initCache(filter);
        this.fireDatasetChanged();
    }

};

