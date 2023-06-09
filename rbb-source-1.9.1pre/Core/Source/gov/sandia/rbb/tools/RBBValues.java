
package gov.sandia.rbb.tools;

import gov.sandia.rbb.EventCache;
import gov.sandia.rbb.RBB;
import gov.sandia.rbb.RBBEventChange;
import gov.sandia.rbb.RBBEventListener;
import gov.sandia.rbb.RBBFilter;
import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.Timeseries;
import gov.sandia.rbb.impl.h2.statics.H2STimeseries;
import java.sql.SQLException;
import java.util.Arrays;

/**
 *
 * RBBValues allows specifying a group of values.
 * Each value is a vector with a name.
 * The group of values is mirrored among all instances connected to the same RBB.
 *<p>
 * Conceptually, a Value differs from a Timeseries in that
 * the assumption is you only care about the current (i.e. latest) value.
 * When a new value is set, the previous value (of the same name) is discarded in the RBB so they don't build up.
 * If a value is set to what it was previously, the call is ignored.
 *<p>
 * In the RBB, a Value is a timeseries.  It has the tags
 * "valueGroup=<groupName>" and "valueName=<valueName>"
 *<p>
 * If you only want a single value, see the SingleValue nested class below.
 *<p>
 * To do event-driven processing when a value changes, use a valueListener.
 * Callbacks are received in different thread for each instance (it is based
 * on RBB addEventListener)
 *
 * @author rgabbot
 */
public class RBBValues {
    private RBB rbb;
    private String groupName;
    private EventCache eventCache;

    /*
     * Retains a reference to the RBB
     */
    public RBBValues(RBB rbb, String groupName_) throws Exception {
        this.rbb = rbb;
        this.groupName = groupName_;
        this.eventCache = new EventCache(rbb);
        eventCache.initCache(RBBFilter.byTags(tagsForGroup()));
    }

    public static RBBValues oneShot(RBB rbb, String groupName_) throws Exception {
        RBBValues v = new RBBValues();
        v.rbb = rbb;
        v.groupName = groupName_;
        v.eventCache = null;
        return v;
    }

    /*
     * Get the value for the specified value within this group.
     * Returns null if none has been set.
     */
    public Float[] getFloats(String name) throws SQLException {

        Timeseries t = findTimeseries(name);
        if(t==null)
            return null;
        else
            return t.getSample(t.getNumSamples()-1).getValue();
    }

    /*
     * Get the specified scalar value (a float), if one has been set.
     * Otherwise return a specified default value (may be null).
     */
    public Float getFloat(String name, Float defaultValue) throws SQLException {
        Float[] f = getFloats(name);
        if(f == null || f.length == 0)
            return defaultValue;
        return f[0];
    }

    public void setFloats(String name, Float... newValue) throws SQLException {

        // setting same value as previous is no-op
        if(Arrays.deepEquals(newValue, getFloats(name)))
            return;

        final double now = (double) System.currentTimeMillis();

        // todo: this ought to be re-done in a threadsafe way, since somebody else
        // could create the Timeseries beteween the time we check for it and the time
        // we create it.        
        Timeseries t = findTimeseries(name);
        if(t == null)
            t = new Timeseries(rbb, newValue.length, now, tagsForValue(name));

        t.add(rbb, now, newValue);

        // truncating the start to the current time causes previously set values to be discarded.
        t.setStart(rbb.db(), now);
    }

    /*
     * set a value, only if there was no value for the name previously.
     */
    public void setDefault(String name, Float... newValue) throws SQLException {
        // todo: the real point of this function is avoid the race condition
        // that the following does not fix...
        if(getFloats(name)==null)
            setFloats(name, newValue);
    }

    /*
     * When the Value changes, listener.eventDataAdded will fire.
     * Other listener notifications should be ignored.
     * You can use getGroupName, getValueName, and getValue below
     * to get the information from the EventDataAdded instance.
     */
    public void addEventListener(RBBEventListener listener) {
        if(eventCache == null)
            throw new IllegalStateException("RBBValues.addEventListener Error: this RBBValues instance was created by oneShot and does not listen for updates");
        eventCache.addEventListener(listener);
    }

    /*
     * After calling addEventListener, the listener callback can
     * create a DataAddedAccessor to make getting the new value
     * a little more convenient / readable.
     */
    public static class DataAddedAccessor {
        RBBEventChange.DataAdded d;

        public DataAddedAccessor(RBBEventChange.DataAdded d) {
            this.d=d;
        }

        public String getGroupName(RBBEventChange.DataAdded d) {
            return d.event.getTagset().getValue("valueGroup");
        }

        public String getValueName(RBBEventChange.DataAdded d) {
            return d.event.getTagset().getValue("valueName");
        }

        public Float[] getValue(RBBEventChange.DataAdded d) {
            return H2STimeseries.getSampleFromRow(d.data);
        }
    }

    public void removeEventListener(RBBEventListener listener) {
        if(eventCache == null)
            throw new IllegalStateException("RBBValues.removeEventListener Error: this RBBValues instance was created by oneShot and does not listen for updates");
        eventCache.removeEventListener(listener);
    }

    public Tagset tagsForGroup() {
        Tagset t = new Tagset();
        t.set("valueGroup", groupName);
        return t;
    }
    
    public Tagset tagsForValue(String valueName) {
        Tagset t = tagsForGroup();
        t.set("valueName", valueName);
        return t;
    }

    /*
     * This class simplifies things when instead of a group of values you just need one.
     * <p>
     * It will appear in the RBB as a Timeseries with the tagset<br>
     * sharedCurrentValueGroup=<name>,valueName=<name>
     */
    public static class SingleValue {
        private String name;
        private RBBValues cv;
        
        public SingleValue(RBB rbb, String name, Float... defaultValue) throws Exception {
            cv = new RBBValues(rbb, name);
            this.name=name;
            cv.setDefault(name, defaultValue);
        }

        public static SingleValue oneShot(RBB rbb, String name, Float... defaultValue) throws Exception {
            SingleValue sv = new SingleValue();
            sv.name = name;
            sv.cv = RBBValues.oneShot(rbb, name);
            sv.cv.setDefault(name, defaultValue);
            return sv;
        }
        private SingleValue() { }

        public Float[] getValue() throws SQLException {
            return cv.getFloats(name);
        }

        public void setValue(Float... value) throws SQLException {
            cv.setFloats(name, value);
        }
    }

    private RBBValues() { }

    /*
     * Find the Timeseries for the specified value.
     * If there are several, returns one arbitrarily
     * (this should not occur if this class is used to set the values)
     */
    private Timeseries findTimeseries(String valueName) throws SQLException {
        RBBFilter f = RBBFilter.byTags(tagsForValue(valueName));
        Timeseries[] ts;
        if(eventCache != null)
            ts = eventCache.findTimeseries(f);
        else
            ts = Timeseries.findWithSamples(rbb.db(), f);

        if(ts.length == 0)
            return null;
        else
            return ts[0];
    }


}
