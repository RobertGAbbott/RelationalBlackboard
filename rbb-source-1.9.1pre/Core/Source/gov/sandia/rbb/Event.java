
package gov.sandia.rbb;

import java.util.Comparator;
import java.util.Collections;
import gov.sandia.rbb.impl.h2.statics.H2STime.TimeCoordinateParameters;
import java.util.Map;
import java.util.HashMap;
import java.sql.Connection;
import gov.sandia.rbb.impl.h2.statics.H2SEvent;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import static gov.sandia.rbb.RBBFilter.*;

/**
 * <pre>
 * An RBB Event is the following:
 * Tagset: a set of name/value pairs.
 * Start Time: when the event started.
 * End Time: when the Event ended.  Is conventionally set to Double.MAX_DOUBLE if not yet known.
 * ID: a long that uniquely identifies the Event within the RBB.
 *
 * Persistent vs. in-RAM changes:
 * Any Event method that persistently changes the Event in the RBB requires an RBB parameter.
 * Any Event method that doesn't take an RBB parameter, does not persistently change the Event in the RBB.
 *
 * Rationale: Since the Event instance "knows" whether it is persistent or not,
 * it could be asked why the Event doesn't just keep a reference to the RBB if it was
 * created persistently, and then update persistently automatically from then on if so.
 * The reasons are:
 * 1) This makes it impossible from looking at a given line of code whether an Event operation is persistent or transient
 * 2) An persistent event may be updated through different Connections (i.e. RBB instances) referring to the same underlying RBB, for example in RBBEventListener H2 creates a new Connection instance for each trigger invocation.
 * 3) Potentially persistent calls would then have to declare "throws SQLException" which is a pain when working with Events nonpersistently.
 *
 * </pre>
 * @author rgabbot
 */

public class Event implements Comparable<Event>, Cloneable {
    protected Long id;
    protected Double start, end;
    protected Tagset tagset;

    /**
     * This tracks whether this Event instance was created persistently, i.e. in the RBB,
     * and throws an exception if the Event is later modified non-persistently.
     * (Or conversely if created nonpersistently and modified persistently).
     * If you don't want this check on a particular
     * Event, set isPersistent to null which disables the check for that instance.
     *
     */
    public Boolean isPersistent;

    /**
     * If the timeConverter is non-null, it is initialized to convert from the tagset's native time coordinate
     * (as it is stored in the RBB), to the output or display time coordinate.
     */
    protected TimeCoordinateParameters timeConverter;

    /*
     * Make a new deep copy of the specified event, including a new Tagset instance.
     */
    public Event(Event e) {
        this(e.id, e.start, e.end, e.getTagset().clone()); // don't use accessors getStart()/getEnd(); that would get the converted times.
        if(e.timeConverter != null)
            timeConverter = new TimeCoordinateParameters(e.timeConverter);
        isPersistent = e.isPersistent;
    }

    /*
     * Construct a persistent instance from pre-existing data.
     * (i.e. it is assumed id is an actual RBB ID)
     */
    public Event(Long id, Double start, Double end, Tagset tagset)
    {
        this.id = id;
        this.start = start;
        this.end = end;
        this.tagset = tagset;
        isPersistent = true;
    }

    /*
     * Construct a transient instance from pre-existing data.
     */
    public Event(Double start, Double end, Tagset tagset)
    {
        this.id = null;
        this.start = start;
        this.end = end;
        this.tagset = tagset;
        isPersistent = false;
    }

    public Event(ResultSet rs) throws SQLException {
        this(rs.getLong("ID"),
            rs.getDouble("START_TIME"), rs.getDouble("END_TIME"),
            new Tagset(rs.getString("TAGS")));
    }

    /*
     * Create an Event persistently (in the database)
     */
    public Event(Connection db, double startTime, double endTime, Tagset tags) throws SQLException {
        this(H2SEvent.create(db, startTime, endTime, tags.toString()), startTime, endTime, tags);
    }

    /*
     * Access the Event with a different time coordinate.
     * f is created with RBBFilter.usingTimeCoordinate()
     *<p>
     * Even though this constructor takes a DB connection, it does NOT create a new
     * persisten Event; the connection is only to look up the time conversion parameters.
     */
    public Event(Event e, RBBFilter f, Connection conn) throws SQLException {
        this(e);
        this.isPersistent = e.isPersistent;
        if(f.timeCoordinate != null) {
            timeConverter = f.getTimeCache().getConversionParameters(conn, e.getTagset(), f.timeCoordinate);
            // if e already had a non-null timeConverter, ours is a composition of theirs
            if(e.timeConverter != null) {
                timeConverter._b += timeConverter._m*e.timeConverter._b;
                timeConverter._m *= e.timeConverter._m;
            }
            tagset.set(f.timeCoordinate); // change my tags to reflect the time coordinate I am presenting.
        }
    }

    /**
     * get from rbb by ID.
     * Throws IllegalArgumentException for invalid ID.
     *<p>
     * If one of the (optional) filters is withTimeCoordinate, then the times (start/end) are converted in the result.
     * If the Event specified by the ID doesn't match any of the filters, it won't be found and the exception will be thrown.
     */
    public static Event getByID(Connection conn, Long id, RBBFilter... f) throws SQLException, IllegalArgumentException {
        Event[] e = find(conn, byID(id), new RBBFilter(f));
        if(e.length == 0)
            throw new IllegalArgumentException("Event.getByID: invalid Event ID "+id);
        return e[0];
    }
 
    /**
     * Retrieve Event instances for the specified IDs.<br>
     * The result is in the same order as the specified IDs.<br>
     * Invalid Event IDs (e.g. IDs of events, or completely bogus IDs) result in null elements in the result.
     * <p>
     * The optional RBBFilter argument can specify a time coordinate, or restrict the result such that only
     * IDs with data in a specified schema are found, etc.
     * 
     * @throws SQLException
     */
    public static Event[] getByIDs(Connection conn, Long[] ids, RBBFilter... filters) throws SQLException {
        // call findWithTimeCoordinate instead of get because we want to use the time cache parameter.
        
        // first put in a hash to sort the results back into an array, since get returns time-ordered without regard to the order of the ids parameter.
        Map<Object,Event> byID = new HashMap<Object,Event>();
        for(Event e : find(conn, new RBBFilter(filters), byID(ids)))
            byID.put(e.getID(), e);

        // now put them in the desired order.
        Event[] result = new Event[ids.length];
        for(int i = 0; i  < ids.length; ++i) {
            Event ev = byID.get(ids[i]); // will be null if the corresponding ID was not an event ID, or other filters passed excluded the event.
            result[i] = ev;
        }

        return result;
    }


    public Long getID()
    {
        return id;
    }

    public Double getStart() {
        return mapTime(start);
    }

    public Double getEnd() {
        return mapTime(end);
    }

    public Tagset getTagset() {
        return tagset;
    }

    /*
     * Set the tagset for this instance, nonpersistently.
     * The previous Tagset instance does not carry over in any way.
     */
    public void setTagset(Tagset t) {
        this.tagset = t;
    }

    /*
     * Set the start time for this instance, nonpersistently.
     */
    public void setStart(double newStart) {
        if(isPersistent != null && isPersistent) // see comment on declaration of isPersistent
            throw new IllegalArgumentException("Event.setStart: the instance was created persistently but was then called without an RBB parameter");

        setInstanceTimes(newStart, null);
    }

    /*
     * Set the end time for this instance, nonpersistently.
     */
    public void setEnd(double newEnd) {
        if(isPersistent != null && isPersistent) // see comment on declaration of isPersistent
            throw new IllegalArgumentException("Event.setEnd: the instance was created persistently but was then called without an RBB parameter");
        setInstanceTimes(null, newEnd);
    }

    /*
     * Set the end for this instance and in the RBB
     */
    public void setStart(Connection db, double startTime) throws SQLException {
        if(isPersistent != null && !isPersistent) // see comment on declaration of isPersistent
            throw new IllegalArgumentException("Event.setStart: the instance was created transiently but was then called with an RBB parameter");
        setInstanceTimes(startTime, null);
        H2SEvent.setStartByID(db, id, this.start);
    }

    /*
     * Set the end for this instance and in the RBB
     */
    public void setEnd(Connection db, double endTime) throws SQLException {
        if(isPersistent != null && !isPersistent) // see comment on declaration of isPersistent
            throw new IllegalArgumentException("Event.setEnd: the instance was created transiently but was then called with an RBB parameter");
        setInstanceTimes(null, endTime);
        H2SEvent.setEndByID(db, id, this.end);
    }
    
    /*
     * Set the start and/or end times in this java instance.
     * Time conversion is performed, but it is not checked whether
     * this is a persistent copy, and the rbb is not modified
     *<p>
     * The reason for this function is so Timeseries can override it
     * to discard Samples before/after the new times.
    */
    protected void setInstanceTimes(Double startTime, Double endTime) {
        if(startTime != null)
            this.start = unMapTime(startTime);
        if(endTime != null)
            this.end = unMapTime(endTime);
    }

    /*
     * Offset the start and end by the specified amount.
     * dt is specified in the display time coordinate.
     * <p>
     * This alters the timeConverter stored in this Event, so it will be effective
     * e.g. on Timeseries Samples that are retrieved from the RBB after this is called.
     */
    public void timeShift(double dt) {
        if(isPersistent != null && isPersistent) // see comment on declaration of isPersistent
            throw new IllegalArgumentException("Event.timeShift: the instance was created persistently but was then called without an RBB parameter");

        if(timeConverter == null)
            timeConverter = new TimeCoordinateParameters();

        timeConverter._b += dt;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append(" id=");
        sb.append(getID());
        sb.append(", start=");
        sb.append(getStart());
        sb.append(", end=");
        sb.append(getEnd());
        sb.append(", tags=(");
        sb.append(getTagset().toString());
        sb.append(")");
        if(timeConverter != null) {
            sb.append(" timeConversion=");
            sb.append(timeConverter.toString());
        }
        return sb.toString();
    }

    /**
     * Get the tagsets from an array of events.
     */
    public static Tagset[] getTagsets(Event[] e) throws SQLException {
        Tagset[] result = new Tagset[e.length];
        for(int i = 0; i < e.length; ++i)
            result[i] = e[i].getTagset();
        return result;
    }

    @Override
    public boolean equals(Object o) {

       if (o == null)
          return false;

       if(this == o)
           return true;

       if(getClass() != o.getClass())
          return false;

       Event e = (Event) o;

       return equals(id, e.getID()) &&
                equals(start, e.getStart()) &&
                equals(end, e.getEnd()) &&
                equals(tagset, e.getTagset());
    }

    private static <T> boolean equals(T a, T b) {
        if(a==null)
            return b==null;
        return a.equals(b);
    }

    /**
     * This is the main function for finding events.
     * Find the events as specified by the conditions in the RBBFind
     * <p>
     * NOTE: the override in Timeseries.find has a different signature, so if you're
     * trying to call Timeseries.find, call Timeseries.findWithSamples or Timeseries.findWithoutSamples instead.
     */
    public static Event[] find(Connection rbb, RBBFilter... f) throws SQLException {
        RBBFilter filter = new RBBFilter(f); // collect filter array f into its intersection.
        ArrayList<Event> result = new ArrayList<Event>();

        if(filter.timeCoordinate == null) {
            ResultSet rs = H2SEvent.findWithoutTimeCoordinate(rbb, new RBBFilter(filter));
            while (rs.next())
                result.add(new Event(rs));
            return result.toArray(new Event[0]);
        }

        // this follows the logic from H2SEvent.find, where there is documentation on the rationale of this.

        RBBFilter filterNoTimeLimits = new RBBFilter(f);
        filterNoTimeLimits.start = filterNoTimeLimits.end = null;
        ResultSet rs = H2SEvent.findWithoutTimeCoordinate(rbb, filterNoTimeLimits);
        while(rs.next()) {
            Event ev = new Event(rs);
            try {
                ev.timeConverter = filter.getTimeCache().getConversionParameters(rbb, ev.getTagset(), filter.timeCoordinate);
            } catch(SQLException e) {
                System.err.println("Event.find warning: failed to retrieve time coordinate parameters for "+ev+"; using unmapped times.");
            }
            if(filter.end != null && ev.getStart() > filter.end) // compare converted time to filter conditions - getStart() calls mapTime()
                continue;
            if(filter.start != null && ev.getEnd() < filter.start)
                continue;
            ev.tagset.set(filterNoTimeLimits.timeCoordinate); // alter the output tagset to reflect the display time coordinate
            result.add(ev);
        }

        Collections.sort(result);

        return result.toArray(new Event[0]);
    }

    /*
     * See H2SEvent.findTagCombinations
     */
    public static Tagset[] findTagCombinations(Connection rbb, String tagNames, String filterTags) throws SQLException {
        ArrayList<Tagset> result = new ArrayList<Tagset>();
        ResultSet rs = H2SEvent.findTagCombinations(rbb, tagNames, filterTags);
        while(rs.next())
            result.add(new Tagset(rs.getString(1)));
        return result.toArray(new Tagset[0]);

    }

    /*
     * See the shortened form of H2SEvent.findTagCombinations
     */
    public static Tagset[] findTagCombinations(Connection rbb, String filterTags) throws SQLException {
        ArrayList<Tagset> result = new ArrayList<Tagset>();
        ResultSet rs = H2SEvent.findTagCombinations(rbb, filterTags);
        while(rs.next())
            result.add(new Tagset(rs.getString(1)));
        return result.toArray(new Tagset[0]);

    }

    /*
     * Copy out the IDs from the events into a Long[], in order.
     */
    public static Long[] getIDs(Event[] events) {
        Long[] ids = new Long[events.length];
        for(int i = 0; i < events.length; ++i)
            ids[i] = events[i].getID();
        return ids;
    }


    public double mapTime(double t) {
        if(timeConverter==null)
            return t;
        else
            return timeConverter.map(t);
    }

    public double unMapTime(double t) {
        if(timeConverter==null)
            return t;
        else
            return timeConverter.unmap(t);
    }

    /*
     * Default sort order is by start time, breaking ties by earliest end time.
     */
    @Override
    public int compareTo(Event o) {
        int c = (int) Math.signum(getStart()-o.getStart());
        if(c != 0)
            return c;
        return (int) Math.signum(getEnd()-o.getEnd());
    }

    /**
     * Compare Events by their Tagsets, breaking ties with the default (time-based) order.
     */
    public static class CompareByTags implements Comparator<Event> {
        TagsetComparator tagsetComparator;
        public CompareByTags(TagsetComparator c) {
            tagsetComparator = c;
        }
        @Override
        public int compare(Event o1, Event o2) {
            int c = tagsetComparator.compare(o1.getTagset(), o2.getTagset());
            if(c != 0)
                return c;
            return o1.compareTo(o2);
        }
    }

    /*
     * make a deep copy, including a new Tagset instance.
     */
    @Override
    public Event clone()
    {
        return new Event(this);
    }

}
