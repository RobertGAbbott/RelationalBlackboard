
package gov.sandia.rbb;

import gov.sandia.rbb.Timeseries.Sample;
import gov.sandia.rbb.impl.h2.statics.H2STimeseries;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The purpose of this class is to reduce repetitive, time-consuming calls 
 * to findEvents().  EventCache keeps a copy of all Events matching a
 * specified RBBFilter that is updated in an event-driven fashion.
 *<p>
 * Events with Timeseries data are represented by a Timeseries instance and
 * so can be used to retrieve Samples.
 *<p>
 * To do event-driven processing, see addEventListener.  It will be invoked
 * by this cache after it updates itself first, and the Event instance in the
 * RBBEventChange will reference the instance from this cache.
 *
 */
public class EventCache implements RBBEventListener {
    protected Map<Long, Event> events;
    protected boolean isInitialized = false;
    protected RBBFilter filter;
    protected RBB rbb;

    /*
     *
     * This instance will keep a reference to the rbb, and synchronize on
     * the Connection (rbb.db()) before making calls on it.  So if you (the caller)
     * are keeping the rbb to use for other purposes, you must also synchronize
     * on the Connection.  Alternately, open a separeate Connection just for
     * each Cache or for all of them to share.
     *
     */
    public EventCache(RBB rbb) {
        this.rbb=rbb;
        listeners = new HashSet<RBBEventListener>();
    }

    /**
     * This is a no-op if the args are the same as for the previous call to init.
     * <p>
     * Note: this only needs to be called to change the set of events for which
     * findEvents will be optimized.  (I.e. it doesn't need to be called just
     * because events might have been added or removed from the RBB, for example)
     *
     */
    public synchronized void initCache(RBBFilter... filters) throws SQLException {
        RBBFilter filter = new RBBFilter(filters);

        if(isInitialized && this.filter.equals(filter))
            return;

        disconnect();

        this.filter = new RBBFilter(filter);

        this.events = new java.util.TreeMap<Long, Event>();

        // get the initial set of Events
        for(Event event : Event.find(rbb.db(), this.filter))
            addEvent(rbb, event);

        System.err.println("Eventcache for "+filter+" found "+events.size()+" initial Events");

        // register to discover future events
        rbb.addEventListener(this, filter);

        isInitialized = true;
    }

    public synchronized void disconnect() throws SQLException {
        if(rbb!=null)
            rbb.removeEventListener(this);
        events = null;
        isInitialized = false;
    }

    /*
     * Find all the events that match the previous call to initEventCache,
     * which may also optionally be limited to those matching extra filters.
     * <p>
     * The returned array is sorted by event start time (as with Event.find())
     */
    public synchronized Event[] findEvents(RBBFilter... filters) throws SQLException {
        RBBFilter f = new RBBFilter(filters);
        ArrayList<Event> result = new ArrayList<Event>();

        if(!isInitialized)
            throw new IllegalStateException("EventCache.findEvents error: initCache has not been called");

        synchronized(rbb.db()) { // synchronized on the connection because f.matches may need to access the RBB
            for(Event e : events.values()) {
                if(f.matches(rbb.db(), e))
                    result.add(e);
            }
        }

        Collections.sort(result);

        return result.toArray(new Event[]{});
    }

    static boolean equals(Object a, Object b) {
        if(a == null)
            return b == null;
        if(b == null)
            return false;
        return a.equals(b);
    }

    /**
     * Implement EventListener interface to keep the cache up to date when events are created, destroyed, modified.
     */
    @Override
    public synchronized void eventAdded(RBB rbb, RBBEventChange.Added ec)
    {
        try
        {
            addEvent(rbb, ec.event);
        }
        catch (SQLException ex)
        {
            isInitialized = false;
            System.err.println("EventCache.eventAdded error on event " + ec.event.toString() + ex.getMessage());
        }
        dispatchToListeners(ec);
    }




    @Override
    public void eventModified(RBB rbb, RBBEventChange.Modified ec)
    {
        try
        {
            Event oldEvent = events.get(ec.event.getID());
            if(oldEvent != null) { // if we already had it, update the old instead of using a new, this way anybody holding a reference to the old one will see the update.
                oldEvent.setInstanceTimes(ec.event.getStart(), ec.event.getEnd());
                oldEvent.tagset = ec.event.tagset;
            }
            else {
                addEvent(rbb, ec.event);
            }
        }
        catch (SQLException ex)
        {
            isInitialized = false;
            System.err.println("EventCache.eventModified error on event " + ec.event.toString() + ex.getMessage());
        }
        dispatchToListeners(ec);
    }

    @Override
    public synchronized void eventRemoved(RBB rbb, RBBEventChange.Removed ec)
    {
        // for eventRemoved, dispatch to listeners first since after that the Event won't exist.
        dispatchToListeners(ec);

        events.remove(ec.event.getID());
    }

    @Override
    public synchronized void eventDataAdded(RBB rbb, RBBEventChange.DataAdded ec)
    {
        try {
            // this is a timeseries so we know it has timeseries data attached,
            // but it might also have other schema attached so we could get notifications for those.
            if(!ec.schemaName.equals(H2STimeseries.schemaName))
                return;

            Timeseries ts = (Timeseries) events.get(ec.event.getID());
            if(ts == null) synchronized(rbb.db()) {
                // We got notified of data added to a timeseries we don't know about, which
                // is normally prevented because initCache() gets all timeseries that
                // existed beforehand.
                // But this does happen if a timeseries is created just after the cache is initialized
                // because the server operates in its own thread, on the network, and didn't
                // receive the subscription request until after the timeseries was created.
                System.err.println("Got Event data for event not in this cache: " + ec.event);
                addEvent(rbb, ec.event);
                ts = getTimeseriesByID(ec.event.getID())[0];
                // we may also have missed the first few samples for the timeseries.
                // this call to getAllSamples will probably get the sample we're currently
                // processing, but that's OK due to the call to alreadyContains, below.
                ts.loadAllSamples(rbb.db());
            }

            if(maxSamples != null && maxSamples == 0) {
                ts.keepNewest(0);
                return;
            }

            Sample sample = new Sample(H2STimeseries.getTimeFromRow(ec.data), H2STimeseries.getSampleFromRow(ec.data));

            if(!alreadyContains(ts, sample))
                ts.addWithoutTimeConversion(sample); // store as received... time conversion is done for presentation.
            if(maxSamples != null)
                ts.keepNewest(maxSamples);
        }
        catch (SQLException ex){
            System.err.println("TimeseriesCache.eventDataAdded exception: "+ex.getMessage());
        }

        dispatchToListeners(ec);
    }

    /*
     * This method is normally just used internally by the class when the RBB notifies
     * it of a new event, but can also be called when a new Event has been created to
     * add it to the cache manually so it will be there immediately, before the RBB
     * calls back to notify the cache.
     */
    public void addEvent(RBB rbb, Event event) throws SQLException {

        Event newEvent;

        synchronized(rbb.db()) {

            // is this a Timeseries?
            Integer dim = H2STimeseries.getDim(rbb.db(), event.getID());

            if(dim == null) { // no, just an Event
                try {
                    // make a copy to reflect the EventCache time coordinate (if necessary) and to set isPersistent
                    newEvent = new Event(event, filter, rbb.db());
                    } catch(SQLException e) {
                        System.err.println("EventCache.addEvent Warning: Error retrieving time coordinate "+filter.timeCoordinate+" for event "+event+"; using unconverted times");
                        newEvent = event.clone();
                        newEvent.timeConverter = null;
                    }
            }
            else {
                Timeseries newTs = new Timeseries(event, dim);
                newEvent = newTs;
                if(maxSamples==null) // no set max = get all.
                    newTs.loadAllSamples(rbb.db());
                else if(maxSamples > 0)
                    newTs.loadRecentSamples(rbb.db(), maxSamples);

                if(filter.timeCoordinate != null)
                    try {
                        newTs = new Timeseries(newTs, filter, rbb.db());
                    } catch(SQLException e) {
                        System.err.println("EventCache.addEvent Warning: Error retrieving time coordinate "+filter.timeCoordinate+" for timeseries "+newTs+"; using unconverted times");
                        newTs.timeConverter = null;
                    }
                }
        }

        // We will consider this copy to be "persistent" since the idea of the TimeseriesCache
        // is for the Timeseries to maintain consistency with the RBB.
        newEvent.isPersistent = true;

        // System.err.println("TimeseriesCache adding new Timeseries: "+ts);

        synchronized(this) { // protect this.events.
            events.put(event.getID(), newEvent);
        }
    }


    /**
     * Returns the number of Events in the RBB that match the filter tags.
     */
    public synchronized int getNumCachedEvents() {
        return events.size();
    }

    /**
     * This implementation of getEventsByID will ONLY include
     * Events matching the arguments for the previous call to initEventCache.
     * The result array will include null values for any IDs not in the cache.
     */
    public synchronized Event[] getEventsByID(Long... IDs) throws SQLException {
        ArrayList<Event> results = new ArrayList<Event>();
        for(Long ID : IDs)
            results.add(events.get(ID));
        return results.toArray(new Event[0]);
    }

    private Integer maxSamples;

    /*
     * If set non-null, only the last N samples per timeseries will be kept.
     * May be 0 for no samples.
     * Be sure to call this *before* initCache if you are working with huge timeseries
     * because initCache will pull down all the timeseries data otherwise.
     *
     */
    public synchronized void setMaxSamples(Integer maxSamples) {
        if(maxSamples.equals(this.maxSamples))
            return;

        // if this fails you have called setMaxSamples after initCache...
        // It must be called beforehand.
        // But you can call destroy(), then setMaxSamples(), then initCache()
        if(isInitialized) {
            System.err.println("TimeseriesCache warning: setMaxSamples called on TimeseriesCache "+filter+" after initCache was already called.  Will have no effect until next call to initCache.");
        }

        this.maxSamples = maxSamples;
    }


    /*
     * Find all the timeseries that match the previous call to initCache,
     * as well as any additional conditions imposed by the optional arguments.
     *
     */
    public Timeseries[] findTimeseries(RBBFilter... filters) throws SQLException {
        Event[] ev = findEvents(filters);
        // the events we just found were all created by addEvent, below, so they are Timeseries instances.
        Timeseries[] timeseries = new Timeseries[ev.length];
        for(int i = 0; i < ev.length; ++i)
            timeseries[i] = (Timeseries) ev[i];
        return timeseries;
    }

    private void dispatchToListeners(RBBEventChange ec) {
        RBBEventChange change = ec.clone();
        // substitute our Event cache instance for the temporary Event from the change notification.
        // Our copy has timeseries conversion if necessary, and is a Timeseries instance
        // if the Event is a Timeseries.
        change.event = events.get(ec.event.getID());
        for(RBBEventListener listener : listeners)
            change.dispatch(rbb, listener);
    }

    /**
     * If Samples are added persistently to a Timeseries from a TimeseriesCache,
     * the RBB will still notify us and we don't want to add twice.
     * <p>
     * Also if this cache object is initialized right after a timeseries has been created and data added to it,
     * then since even change notification is asynchronous, it can getCopy eventDataAdded notification
     * for data that was already retrieved during initialization.
     **/
    private boolean alreadyContains(Timeseries timeseries, Sample s0) throws SQLException {
        // this looks like a linear search, but unless data is added in the wrong order
        // the loop will never even run once.
        for(int i = timeseries.getNumSamples()-1; i >= 0; --i) {
            Sample s1 = timeseries.getSampleWithoutTimeConversion(i);
            if(s1.getTime() < s0.getTime())
                return false;
            if(s1.getTime().equals(s0.getTime())) // since we got the sample with no time conversion, should match exactly if it's really the same sample.
                return s1.equals(s0);
        }
        return false;
    }

    /**
     * This will ONLY include Timeseries matching the arguments for the previous call to initCache.
     * The result array will include null values for any IDs not in the cache, or that are not timeSeries
     */
    public Timeseries[] getTimeseriesByID(Long... IDs) throws SQLException {
        Event[] events = getEventsByID(IDs);
        Timeseries[] result = new Timeseries[events.length];
        for(int i = 0; i < result.length; ++i) {
            if(!(events[i] instanceof Timeseries))
                continue; // leave result[i] as null
            result[i] = (Timeseries) events[i];
        }
        return result;
    }

    Set<RBBEventListener> listeners;

    /*
     * The listener will be notified of changes to Events/Timeseries in this cache.
     * Note that the set of listeners persists through calls to initCache.
     */
    public synchronized void addEventListener(RBBEventListener listener)
    {
        listeners.add(listener);
    }

    public synchronized void removeEventListener(RBBEventListener listener)
    {
        listeners.remove(listener);
    }
}
