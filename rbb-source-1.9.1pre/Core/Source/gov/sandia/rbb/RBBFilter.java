
package gov.sandia.rbb;

import gov.sandia.rbb.impl.h2.statics.H2SEvent;
import gov.sandia.rbb.impl.h2.statics.H2STime;
import gov.sandia.rbb.util.StringsWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 *
 * RBBFilter specifies a set of Events on the basis of tags, time, ID, or
 * attached data (e.g. timeseries data).
 *<p>
 * The basic problem addressed by this class is that RBB Events have
 * several attributes (start/end time, tags, ID) any of which may
 * be used to select events for processing.  So methods that require sets
 * of events get very clunky when using positional parameters for every possible
 * criteria.
 *<p>
 * An RBBFilter has a string representation.  A tagset string is also an
 * RBBFilter string.
 *<p>
 * To use this class:
 * <pre>
 * import static gov.sandia.rbb.RBBFilter.*;
 *
 * Then:
 * Event[] events = Event.find(rbb, byTags("type=position"), byTime(0,1));
 * or:
 * Timeseries[] ts = Timeseries.findWithSamples(rbb, byTags("type=position"));
 *</pre>
 * @author rgabbot
 */
public class RBBFilter {

    public Tagset[] tags;
    public Double start, end;
    public Long[] IDs;
    public Tagset timeCoordinate;
    public String attachmentInSchema;

    /**
     * Results are limited to Events having at least these tags.
     * t may be any tagset representation understood by Tagset.toTagsets(), e.g. "x=y" or a Tagset etc.
     * Tag names may not be be null, but a null tag value will match on name only (regardless of value).
     * If more than one tagset is specified, get events matching any of the tagsets,
     * returning each Event only once even if it matches multiple tagsets.
     *<p>
     * The empty Filter (which admits everything) is returned (with a null rather than empty array of tags)
     * if t has no non-empty tagsets, since those would be indistinguishable
     * (semantically and syntactically), to make toString/fromString unambiguous.
     */
    public static RBBFilter byTags(Object t) {
        Tagset[] tags = Tagset.toTagsets(t);
        RBBFilter f = new RBBFilter();

        // check for the condition of nothing but empty tagsets.
        int nonEmptyTagsets=0;
        for(Tagset t0 : tags)
            if(!t0.getNames().isEmpty())
                ++nonEmptyTagsets;
        if(nonEmptyTagsets==0)
            return f;

        f.tags = tags;
        return f;
    }

    /**
     * Set the start time of interest - ignore Events ending before this time.
     */
    public static RBBFilter byStart(Double t) {
        RBBFilter f = new RBBFilter();
        f.start = t;
        return f;
    }

    /**
     * Set the end time of interest - ignore Events start after this time.
     */
    public static RBBFilter byEnd(Double t) {
        RBBFilter f = new RBBFilter();
        f.end = t;
        return f;
    }

    /*
     * Set the start and end time - ignore Events ending before this start time or starting after this end time.
     */
    public static RBBFilter byTime(Double start, Double end) {
        RBBFilter f = new RBBFilter();
        f.start = start;
        f.end = end;
        return f;
    }

    /*
     * Results are limited to events whose IDs are specified.
     * But note that the results will not necessarily be in the same order as the specified IDs,
     * and invalid IDs are silently ignored.
     */
    public static RBBFilter byID(Long... IDs) {
        RBBFilter f = new RBBFilter();
        f.IDs = IDs;
        return f;
    }

    /**
     * Results are limited to events that have attached data in the specified schema.
     */
    public static RBBFilter bySchema(String dataSchema) {
        RBBFilter f = new RBBFilter();
        f.attachmentInSchema = dataSchema;
        return f;
    }

    /*
     * Assume start/end query times are in this time coordinate (e.g. "timeCoordinate=ConditionSeconds")
     * and convert Event start/end times of results to specified coordinate.
     */
    public static RBBFilter withTimeCoordinate(String timeCoordinate) {
        RBBFilter f = new RBBFilter();
        if(timeCoordinate != null)
            f.timeCoordinate = new Tagset(timeCoordinate);
        return f;
    }

    public static RBBFilter withTimeCoordinate(Tagset timeCoordinate) {
        RBBFilter f = new RBBFilter();
        if(timeCoordinate != null)
            f.timeCoordinate = timeCoordinate;
        return f;
    }

    /*
     * Make the intersection of the specified conditions.
     * When they conflict (such as two that specify different start times), the last one wins.
     * Makes a deep copy so nothing mutable is shared with the source conditions.
     */
    public RBBFilter(RBBFilter... conditions) {
        for(RBBFilter f : conditions)
            also(f);
    }

    /*
     * Convert this object to a one-line string representation.
     * Note: this is used programmatically within the code, so it must
     * match fromString and not be changed willy-nilly.
     * However it is also meant to be human-readable.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        // tagset.toString() will not return a string containing an : or ;
        // so we use these as delimeters.

        if(IDs != null) {
            if(sb.length()>0) sb.append(";");
            sb.append("IDs:");
            for(int i = 0; i < IDs.length; ++i) {
                if(i>0) sb.append(",");
                sb.append(IDs[i]);
            }
        }

        if(attachmentInSchema != null) {
            if(sb.length()>0) sb.append(";");
            sb.append("Schema:");
            sb.append(attachmentInSchema);
        }

        if(start != null) {
            if(sb.length()>0) sb.append(";");
            sb.append("Start:");
            sb.append(start);
        }

        if(end != null) {
            if(sb.length()>0) sb.append(";");
            sb.append("End:");
            sb.append(end);
        }

        if(timeCoordinate != null) {
            if(sb.length()>0) sb.append(";");
            sb.append("timeCoordinate:");
            sb.append(timeCoordinate.toString());
        }

        if(tags != null) {
            if(sb.length()>0) sb.append(";");
            sb.append(Tagset.multiToString(tags));
        }

        return sb.toString();
    }

    public static String[] toStrings(RBBFilter[] f) {
        String[] result = new String[f.length];
        for(int i =0; i < f.length; ++i)
            result[i] = f[i].toString();
        return result;
    }

    /*
     * inverse of toString
     */
    public static RBBFilter fromString(String s) {
        RBBFilter f = new RBBFilter();
        if(s.equals(""))
            return f;

        ArrayList<Tagset> tags = new ArrayList<Tagset>();

        for(String field : s.split(";",-1)) {
            String[] subFields = field.split(":",-1);
            if(subFields==null || subFields.length==0)
                continue;
            if(subFields.length==1) // all fields except a tagset contain : whereas an encoded taglist cannot.
                tags.add(new Tagset(subFields[0]));
            else if(subFields.length != 2)
                throw new IllegalArgumentException("RBBFilter.fromString: bad format starting at: " + subFields[0]);           
            else if(subFields[0].equalsIgnoreCase("IDs")) {
                ArrayList<Long> ids = new ArrayList<Long>();
                if(!subFields[1].equals("")) // empty list of IDs is allowed, though pointless.
                    for(String idString : subFields[1].split(","))
                        ids.add(Long.parseLong(idString));
                f.IDs = ids.toArray(new Long[0]);
            }
            else if(subFields[0].equalsIgnoreCase("Schema"))
                f.attachmentInSchema = subFields[1];
            else if(subFields[0].equalsIgnoreCase("Start"))
                f.start = Double.parseDouble(subFields[1]);
            else if(subFields[0].equalsIgnoreCase("End"))
                f.end = Double.parseDouble(subFields[1]);
            else if(subFields[0].equalsIgnoreCase("timeCoordinate"))
                f.timeCoordinate = new Tagset(subFields[1]);
            else
                throw new IllegalArgumentException("RBBFilter.fromString: unrecognized field "+subFields[0]+" of "+subFields.length+" fields: "+StringsWriter.join("**", subFields));
        }

        if(tags.size() > 0)
            f.tags = tags.toArray(new Tagset[0]);

        return f;
        
    }

    public static RBBFilter[] fromStrings(Object[] a) {
        RBBFilter[] result = new RBBFilter[a.length];
        for(int i = 0; i < result.length; ++i)
            result[i] = fromString(a[i].toString());
        return result;
    }

    /**
     * Add the restrictions of f to this.
     */
    public void also(RBBFilter f) {
        if(f.tags != null) {
            tags = new Tagset[f.tags.length]; // deep copy
            for(int i = 0; i < tags.length; ++i)
                tags[i] = f.tags[i].clone();
        }
        if(f.start != null)
            start = f.start;
        if(f.end != null)
            end = f.end;
        if(f.IDs != null)
            IDs = Arrays.copyOf(f.IDs, f.IDs.length); // deep copy
        if(f.timeCoordinate != null)
            timeCoordinate = f.timeCoordinate;
        if(f.attachmentInSchema != null)
            attachmentInSchema = f.attachmentInSchema;
        if(f.timeCache != null) // also inherit the time cache, if any. 
            timeCache = f.timeCache;
    }

    @Override
    public boolean equals(Object other){
        // each test is in a separate clause so you can tell in the debugger which is failing.

        if (other == null)
            return false;
        if (other == this)
            return true;
        if (!(other instanceof RBBFilter))
            return false;
        RBBFilter f = (RBBFilter) other;
        if(!(Arrays.deepEquals(f.tags, tags)))
            return false;
        if(!(Arrays.deepEquals(f.IDs, IDs)))
            return false;
        if(!equals(f.attachmentInSchema, attachmentInSchema))
            return false;
        if(!equals(f.start, start))
            return false;
        if(!equals(f.end, end))
            return false;
        if(!equals(f.timeCoordinate, timeCoordinate))
            return false;

        return true;
    }

    private boolean equals(Object o1, Object o2) {
        if(o1 == o2)
            return true;
        if(o1==null || o2==null)
            return false;
        return o1.equals(o2);
    }

    /*
     * The Connection is used only if timeCoordinates and start or end, or bySchema is in use; otherwise it can be null.
     */
    public boolean matches(Connection conn, Event e) throws SQLException {
        if(tags != null && !matchesTags(e.getTagset()))
            return false;
        if(IDs != null && !matchesID(e.getID()))
            return false;
        if(attachmentInSchema != null && H2SEvent.attachedDataTables(conn,e.getID(),attachmentInSchema).length == 0)
            return false;

        if(start != null) {
            double eventEnd = e.getEnd();
            if(timeCoordinate != null)
                eventEnd = getTimeCache().convert(conn, eventEnd, e.getTagset(), timeCoordinate);
            if(eventEnd < start)
                return false;
        }

        if(end != null) {
            double eventStart = e.getStart();
            if(timeCoordinate != null)
                eventStart = getTimeCache().convert(conn, eventStart, e.getTagset(), timeCoordinate);
            if(eventStart > end)
                return false;
        }

        return true;
    }

    private boolean matchesTags(Tagset t) {
        for(Tagset t0 : tags)
            if(t0.isSubsetOf(t))
                return true;
        return false; // if have filter tags but none match
    }

    private boolean matchesID(Long id) {
        for(Long myID : IDs)
            if(myID.equals(id))
                return true;
        return false;
    }

    private H2STime.Cache timeCache;
    public H2STime.Cache getTimeCache() {
        if(timeCache==null)
            timeCache = new H2STime.Cache();
        return timeCache;
    }

    public String getTimeCoordinateString() {
        return timeCoordinate==null ? null : timeCoordinate.toString();
    }

    public String getTagsString() {
        return tags==null ? null : Tagset.multiToString(tags);
    }

    public int getNumTagsets() {
        if(tags==null)
            return 0;
        else
            return tags.length;
    }

    /**
     * This is for Filters with multiple tagsets.
     * It returns an array with a separate RBBFilter for each of the tagsets.  The tagset is a deep copy.
     * If this filter's tagets are 0 or null, returns an empty array.
     */
    public RBBFilter[] getTagDisjunction() {
        if(tags == null || tags.length == 0)
            return new RBBFilter[0];

        RBBFilter[] result = new RBBFilter[tags.length];
        for(int i = 0; i < tags.length; ++i) {
            result[i] = new RBBFilter(this);
            result[i].tags = new Tagset[] { tags[i].clone() }; // deep copy
        }

        return result;
    }

}
