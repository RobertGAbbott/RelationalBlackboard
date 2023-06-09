/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.impl.h2.statics;

import gov.sandia.rbb.Event;
import gov.sandia.rbb.RBB;
import gov.sandia.rbb.RBBFilter;
import static gov.sandia.rbb.RBBFilter.*;
import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.PreparedStatementCache;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.HashSet;
import org.h2.tools.SimpleResultSet;

/**
 *
 * H2SEvent contains static methods that are stored procedures in the H2 implementation of RBB Events
 * The create_rbb.sql script creates aliases for these so they can be called through SQL.
 * The public functions here only accept / return datatypes for which H2 has a SQL mapping.
 * Most functionality is placed here, with the RBB interface implementation (H2Event) being a thin wrapper over them.
 *
 * @author rgabbot
 *
 */

public class H2SEvent
{
    /**
     *
     * convert 'time' from the specified timeCoordinate to the native timeCoordinate of the specified event.
     * All parameters must be non-null.
     *
     * @param conn
     * @param time
     * @param timeCoordinate
     * @param id
     * @return
     * @throws SQLException
     */
    public static double convertTime(Connection conn,
        double time,
        String timeCoordinate,
        long id)
        throws SQLException
    {
          return H2STime.convert(conn, time, timeCoordinate, getTagsByID(conn, id));
    }

    /**
     * Find all combinations of Event tag values in the RBB.
     *<p>
     * This is simply H2STagset.findTagCombination, except tagsets not belonging
     * to any Event are excluded.
     *<p>
     * See H2STagset.findTagCombinations for more documentation.
     *
     *
     * @param conn
     * @param tags restrict results to tagsets matching the filter tags.
     * @param inTableColumn may be null; otherwise it restricts results to tagsets whose ids are found in the specified table and column, e.g. "RBB_EVENTS.TAGSET_ID"
     *
     * @return
     * @throws SQLException
     */
    public static ResultSet findTagCombinations(Connection conn, String tagNames, String filterTags) throws SQLException {
        return H2STagset.findCombinations(conn, tagNames, filterTags, "RBB_EVENTS", "TAGSET_ID");
    }

    /*
     * This calls the simplified form of H2STagset.findCombination in which all null-valued
     * tags in filterTags are expanded/enumerated.  See H2STagset.findCombinations for more documentation.
     */
    public static ResultSet findTagCombinations(Connection conn, String filterTags) throws SQLException {
        return H2STagset.findCombinations(conn, filterTags, "RBB_EVENTS", "TAGSET_ID");
    }

    /***
     *  defines a set of time coordinates, one for each combination of the join tags,
     *  that occur in events matching the (optional) filter tags.
     * e.g.
     * call rbb_define_time_coordinates_for_event_combinations('conditionSeconds', 'experiment,session,condition', 'variable=compositeScore');
     * @param conn
     * @param coordinateName, e.g. 'conditionSeconds' (without 'timeCoordinate=')
     * @param filterTags include only tagsets matching this filter tag set.   If null, all tagsets are included.
     * @param joinTags return rows will be all combinations of values that occur for these tag types, e.g. 'experiment,session'
     * @param timeScale ratio of time units for new time coordinate vs. UTC.  E.g. 1000.0 if new coordinate is milliseconds and UTC is in seconds.
     * @return
     * @throws SQLException
     */
    public static int defineTimeCoordinatesForEventCombinations(
        Connection conn,
        String coordinateName,
        String tagNames,
        String filterTags,
        double timeScale)
        throws SQLException
    {
        ResultSet rs = H2SEvent.findTagCombinations(conn, tagNames, filterTags);
        int n = 0;
        while (rs.next())
        {
            System.err.println("Creating a " + coordinateName + " time coordinate conditioned on " + rs.getString(1));
            // get earliest time at which this combination of tags occurred.
            String q = "select start_time, tags from rbb_find_events(?, null, null, null) limit 1";
            PreparedStatement prep = conn.prepareStatement(q);
            Tagset timeCoordinateTags = new Tagset(rs.getString("TAGS"));
//            prep.setObject(1, timeCoordinateTags.toArray());
            prep.setObject(1, timeCoordinateTags.toString());
            ResultSet rs2 = prep.executeQuery();
            if (!rs2.next()) {
                // shouldn't happen, since findTagSetGroups only returns existing tagsets
                throw new SQLException("Got unexpected empty result set looking for events with tags: "+rs.getString("TAGS"));
            }
            double start = rs2.getDouble(1);
            String tagsOfEarliest = rs2.getString("TAGS");
            try
            {
                start = H2STime.toUTC(conn, start, tagsOfEarliest);
                timeCoordinateTags.set("timeCoordinate", coordinateName);
                H2STime.defineCoordinate(conn, timeCoordinateTags.toString(),
                    timeScale, -start * timeScale); // start is negative because 'start' is in UTC coordinates and this is the offset to convert TO utc.
            }
            catch (Exception e)
            {
                System.err.println("Error defining time coordinate for tags " +  tagsOfEarliest + ": " + e.toString());
            }
            ++n;
        }
        return n;
    }

    public static long create(Connection conn,
        double startTime,
        double endTime,
        String tagset)
        throws SQLException
    {
        return create(conn, H2SRBB.nextID(conn), startTime, endTime, tagset);
    }


    static long create(Connection conn,
        long id,
        double startTime,
        double endTime,
        String tagset)
        throws SQLException
    {
        synchronized(conn) {
            // create must be done BEFORE inserting into RBB_EVENTS, because the RBB_EVENTS calls a trigger that retrieves and uses the tags.
            final long tagsetID = H2STagset.toID(conn, tagset);

            PreparedStatementCache.Query q = PreparedStatementCache.startQuery(conn);
            q.add("insert into RBB_EVENTS values");
            q.addParamArray(id, startTime, endTime, tagsetID);
            q.getPreparedStatement().execute();
        }
        return id;
    }



    /**
     *<pre>
     * This is the main function for finding events through a SQL call.
     * (If calling from java, look at Event.)
     *
     * if getTagsets==true, this returns:
     * ID        START_TIME        END_TIME        TAGS                     DATA_SCHEMA     DATA_TABLE  DATA_COLS
     * 1         3600.0            7260.0          test=x,timeCoordinate=ms RBB_TIMESERIES  S33         4
     *
     * if getTagsets==false, this returns:
     * ID        START_TIME        END_TIME        TAGSET_ID                DATA_SCHEMA     DATA_TABLE  DATA_COLS
     * 1         3600.0            7260.0          9385                     RBB_TIMESERIES  S33         4
     *
     *
     * The DATA_XXX columns are returned only if dataTableSchema is non-null.
     *
     * Time Conversions - if the timeCoordinate argument is not null:
     *   The returned start and end times are converted to the time coordinate specified in the tagset, if any.
     *   The results are sorted by start time, but before any time conversion is performed, so returning Events with mixed time coordinates may be confusing.
     *   The returned start and end times are not clamped to the specified start and end times (if any), so e.g. calling get with a start time of 0 could return an Event with a start time of -1 and an end time > 0.
     *   The returned tagset contains the specified timeCoordinate, replacing any previous value of this tag.
     *
     * Using time conversions de-optimizes time filtering, so this implementation is slow in situations  when many sequences match the filter tags
     * but only a few match the time constraints.
     *</pre>
     * @param taglist: If not null, results are limited to Events having at least these tags.  Tag names may not be be null, but a null tag value will match on name only (regardless of value).  The taglist can also be an array of tagsets (each of which is an array), which returns the set of events matching any of the tagsets.
     * @param start: If not null, results are limited to Events ending at or after this time.
     * @param end: If not null, results are limited to Events starting at or before this time.
     * @param IDs: If not null, results are limited to events whose IDs are specified.
     * @param timeCoordinate: if not null, convert Event start/end times to specified coordinate, and assume start/end query times are in this time coordinate (e.g. "timeCoordinate=ConditionSeconds")
     * @param dataTableSchema: if non-null, only events with a data table in the specified schema are found.  DATA_SCHEMA and DATA_TABLE identify the linked data table and DATA_COLS specifies the number of columns in the DATA_TABLE.
     */
    public static ResultSet find(Connection conn,
        String taglist,
        Double start,
        Double end,
        Object[] IDs,
        String timeCoordinate,
        String dataTableSchema)
        throws SQLException
    {
        RBBFilter f = new RBBFilter();
        if(taglist != null)
            f.also(byTags(taglist));

        if(start != null)
            f.also(byStart(start));

        if(end != null)
            f.also(byEnd(end));

        if(IDs != null)
            f.also(byID(H2SRBB.makeLongs(IDs)));

        if(timeCoordinate != null)
            f.also(withTimeCoordinate(timeCoordinate));

        if(dataTableSchema != null)
            f.also(bySchema(dataTableSchema));

        if (conn.getMetaData().getURL().equals("jdbc:columnlist:connection"))
            return createResultSet(f.attachmentInSchema != null);
        else
            return find(conn, f);
    }

    /**
     * short version of get for calling through SQL
     *<p>
     * If calling from java, find(connection, RBBFilter) is suggested instead.
     */
    public static ResultSet find(Connection conn,
        String taglist,
        Double start,
        Double end,
        String timeCoordinate) throws SQLException {
        return find(conn, taglist, start, end, null, timeCoordinate, null);
    }

    private static SimpleResultSet createResultSet(boolean withAttachment) {
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("ID", java.sql.Types.BIGINT, 20, 0);
        rs.addColumn("START_TIME", java.sql.Types.DOUBLE, 20, 0);
        rs.addColumn("END_TIME", java.sql.Types.DOUBLE, 20, 0);
        rs.addColumn("TAGS", java.sql.Types.VARCHAR, 20, 0);
        if(withAttachment) {
            rs.addColumn("DATA_SCHEMA", java.sql.Types.VARCHAR, 20, 0);
            rs.addColumn("DATA_TABLE", java.sql.Types.VARCHAR, 20, 0);
            rs.addColumn("DATA_COLS", java.sql.Types.INTEGER, 20, 0);
        }
        return rs;
    }

    /**
     * This is the main function for finding events.
     * Find the events as specified by the conditions in the RBBFind
     */
    public static ResultSet find(Connection conn, RBBFilter... f) throws SQLException {
        RBBFilter filter = new RBBFilter(f); // collect filter array f into its intersection.

        if(filter.timeCoordinate == null)
            return findWithoutTimeCoordinate(conn, new RBBFilter(filter));

        /**
         * Implement searching involving a time coordinate by first
         * doing a search with no time conditions (start and end are null) and then
         * converting and checking time bounds afterwards.
         * This strategy is driven by two things:
         *
         * 1) Retrieving events for all time and then throwing out ones outside the time range of
         * interest seems wasteful, but this is unavoidable when time Coordinates are used, because
         * the time conversion for each Event might be different, so it can't be indexed (unless
         * every Event were to be indexed by every type of time coordinate)
         *
         * 2) Doing time conversions on the fly within the SQL query was *extremely* slow,
         * about a factor of 100 slower than using a time coordinate cache.
         *
         */

        RBBFilter filterNoTimeLimits = new RBBFilter(f);
        filterNoTimeLimits.start = filterNoTimeLimits.end = null;
        ResultSet rs = findWithoutTimeCoordinate(conn, filterNoTimeLimits);

        class Row implements Comparable<Row> {
            Double t; Object[] values;
            Row(Double t, Object... values) { this.t=t; this.values=values; }
            @Override public int compareTo(Row row) {
                return t.compareTo(row.t);
            }
         }

        ArrayList<Row> result = new ArrayList<Row>();
        while(rs.next()) {
            Tagset resultTags = new Tagset(rs.getString("TAGS"));
            Double tcStart = filter.getTimeCache().convert(conn, rs.getDouble("START_TIME"), resultTags, filter.timeCoordinate);
            if(filter.end != null && tcStart > filter.end)
                continue;
            Double tcEnd = filter.getTimeCache().convert(conn, rs.getDouble("END_TIME"), resultTags, filter.timeCoordinate);
            resultTags.set(filter.timeCoordinate); // alter the resultTags to reflect the time coordinate in which data was retrieved.
            if(filter.start != null && tcEnd < filter.start)
                continue;
            if(filter.attachmentInSchema != null)
                result.add(new Row(tcStart, rs.getObject(1), tcStart, tcEnd, resultTags.toString(), rs.getObject(5), rs.getObject(6), rs.getObject(7)));
            else
                result.add(new Row(tcStart, rs.getObject(1), tcStart, tcEnd, resultTags.toString()));

        }

        Collections.sort(result);
        SimpleResultSet rsOut = createResultSet(filter.attachmentInSchema != null);
        for(Row row : result)
            rsOut.addRow(row.values);

        return rsOut;
        
    }

    /*
     * Get events specified by this finder, but ignoring the time coordinate even
     * if one has been specified.
     */
    public static ResultSet findWithoutTimeCoordinate(Connection conn, RBBFilter f) throws SQLException {
        PreparedStatementCache.Query q = PreparedStatementCache.startQuery(conn);

        q.add("select ID, START_TIME, END_TIME, RBB_ID_TO_TAGSET(E.TAGSET_ID) as TAGS");

        if(f.attachmentInSchema != null)
            q.add(", SCHEMA_NAME as DATA_SCHEMA, TABLE_NAME as DATA_TABLE, (SELECT count(*) FROM information_schema.columns where TABLE_SCHEMA = D.SCHEMA_NAME and TABLE_NAME = D.TABLE_NAME) as DATA_COLS");
        q.add(" from RBB_EVENTS E");

        if(f.IDs != null) {
            q.add(" join table(IDS LONG=");
            q.addParamArray((Object[])f.IDs);
            q.add(") on E.ID=IDS");
        }

        if(f.attachmentInSchema != null)
            q.add(" join RBB_EVENT_DATA D on D.EVENT_ID = E.ID and D.SCHEMA_NAME = '"+f.attachmentInSchema+"'");

        if(f.tags != null && f.tags.length > 0) { // taglist is null or empty means the same thing - all events match.
            q.add(" join (");
            H2STagset.hasTagsQuery(conn, f.getTagsString(), q);
            q.add(") T on T.TAGSET_ID = E.TAGSET_ID");
        }

        // time constraints
        if(f.start != null || f.end != null)
            q.add(" where ");

        if (f.start != null) {
            q.add("END_TIME >= ");
            q.addParam(f.start);
        }

        if (f.end != null) {
            if (f.start != null)
                q.add(" and ");

            q.add("START_TIME <= ");
            q.addParam(f.end);
        }


        q.add(" order by START_TIME"); // note, this doesn't mean much if the matching sequences have varying time coordinates.

        // System.out.println("Query: "+q);

        return q.getPreparedStatement().executeQuery();
    }

    /**
     * Find the first event that matches the tagset and
     * starts after 'startsAfter,' by at least minTimeDelta.
     * If there are multiple such events, one is chosen arbitrarily.
     *
     * Returned columns are:
     * ID  	START_TIME  	END_TIME  	TAGS
     * With 1 row, or 0 rows if no such event exists.
     * If there are multiple such events, one is chosen arbitrarily.
     *
     * If minTimeDelta is null, a default of 1e-8 is used.
     * It is not recommended to use a minTimeDelta of 0.0 because 'startsAfter'
     * itself may be returned due to internal rounding error.
     */
    public static ResultSet findNext(Connection conn,
        String tagset,
        Double startsAfter,
        Double minTimeDelta) throws SQLException {
        return findNext(conn, tagset, startsAfter, true, minTimeDelta);
    }

    /**
     * Find the last event that matches the tagset and
     * starts before 'startsBefore,' by at least minTimeDelta.
     * Returned columns are:
     * ID  	START_TIME  	END_TIME  	TAGS
     * With 1 row, or 0 rows if no such event exists.
     *
     * If minTimeDelta is null, a default of 1e-8 is used.
     * It is not recommended to use a minTimeDelta of 0.0 because 'startsBefore'
     * itself may be returned due to internal rounding error.
     */
    public static ResultSet findPrev(Connection conn,
        String tagset,
        Double startsBefore,
        Double minTimeDelta) throws SQLException {
        return findNext(conn, tagset, startsBefore, false, minTimeDelta);
    }


    public static ResultSet findNext(Connection conn,
        String tagset,
        Double startTime,
        boolean goForwards,
        Double minTimeDelta) throws SQLException {
        if(minTimeDelta == null)
            minTimeDelta = 1e-3 + 1e-8; // the time mechanism is only precise to the millisecond!
        if(!goForwards)
            minTimeDelta = -minTimeDelta;
        ArrayList<Object> psSlots = new ArrayList<Object>();

        PreparedStatementCache.Query q = PreparedStatementCache.startQuery(conn);
        q.add("select ID, START_TIME, END_TIME, RBB_ID_TO_TAGSET(TAGSET_ID) as TAGS from rbb_events where tagset_id in (");
        H2STagset.hasTagsQuery(conn,tagset,q);
        q.add(") and start_time ");
        q.add(goForwards ? ">" : "<");
        q.add(Double.toString(startTime+minTimeDelta));
        q.add(" order by start_time ");
        q.add(goForwards? "" : " desc ");
        q.add("limit 1");
        return q.getPreparedStatement().executeQuery();
    }

    public static int delete(Connection conn, RBBFilter... filter)
        throws SQLException
    {
        ResultSet s = find(conn, filter);
        int n = 0;
        while (s.next())
        {
            deleteByID(conn, s.getLong(1));
            ++n;
        }
        return n;

    }

    /**
     *
     * Delete the data for an event from a schema.  It is not an error to specify nonexistent schema or eventID - in this case nothing is done.
     * If eventID is null, deletes the data for all events from the schema, plus the schema itself.
     * If schema is null, deletes the data for eventID from all schemas.
     * eventID and schema cannot both be null or an exception is raised and nothing is done.
     *<p>
     * Note: schema names are case sensitive (todo: currently not, when deleting entire schema!  Should use quoted.
     *<p>
     * @param schema is the schema from which to delete data.  If null, delete data for the event(s) from all schema.
     * @param eventID identifies the event for which to delete the data.  If null, delete all data from the schema(s)
     *
     * @throws SQLException
     */
    public static void deleteData(
            Connection conn,
            String schema,
            Long eventID)
        throws SQLException
    {
        if(schema == null && eventID == null)
            throw new SQLException("H2SEvent.deleteData: 'schema' and 'eventID' cannot both be null");

        ////// first, drop tables.
        // if eventID is null, then we are dropping schemas.
        if(eventID == null) {
            conn.createStatement().execute("DROP SCHEMA IF EXISTS \""+schema+"\";"+
                "DELETE FROM RBB_EVENT_DATA WHERE SCHEMA_NAME='"+schema+"';");
        }

        // now delete individual rows
        PreparedStatementCache.Query q = PreparedStatementCache.startQuery(conn);
        q.addAlt("select SCHEMA_NAME, TABLE_NAME from RBB_EVENT_DATA where EVENT_ID=", eventID);
        if(schema != null)
            q.addAlt(" AND SCHEMA_NAME=", schema);
        ResultSet rs = q.getPreparedStatement().executeQuery();

        while (rs.next()) {
            PreparedStatementCache.Query deleteRows = PreparedStatementCache.startQuery(conn);
            deleteRows.add("delete from \"", rs.getString(1), "\".\"", rs.getString(2), "\" where EVENT_ID=");
            deleteRows.addParam(eventID);
            deleteRows.getPreparedStatement().execute();
        }
        rs.close();

        PreparedStatementCache.Query deleteLinks = PreparedStatementCache.startQuery(conn);

        deleteLinks.add("DELETE FROM RBB_EVENT_DATA WHERE EVENT_ID=");
        deleteLinks.addParam(eventID);
        if(schema != null) {
            deleteLinks.add(" AND SCHEMA_NAME=");
            deleteLinks.addParam(schema);
        }
        deleteLinks.getPreparedStatement().execute();
    }

    public static int deleteByID(Connection conn, long id)
        throws SQLException
    {
        StringWriter q = new StringWriter();

        // drop associated rows.
        ResultSet rs = conn.createStatement().executeQuery("select SCHEMA_NAME, TABLE_NAME from RBB_EVENT_DATA where EVENT_ID = " + id);
        while (rs.next())
            q.write("delete from \"" + rs.getString("SCHEMA_NAME") + "\".\"" + rs.getString("TABLE_NAME") + "\" where EVENT_ID=" + id + ";");

        // delete the event itself.
        q.write("delete from RBB_EVENTS where id = " + id + ";");

        // drop the data associations themselves.
        // Note this must be done AFTER deleting the Event, because listeners whose
        // interest depends on whether the event has data in a particular schema
        // (such as TimeseriesCache) will not be notified if this association is
        // dropped before the event is deleted.
        q.write("delete from RBB_EVENT_DATA where EVENT_ID = " + id + ";");

        conn.createStatement().execute(q.toString());

        ////////// cleanup the tagset, and strings table
//        if (!H2SRBB.tagsetInUse(conn, TAGSET_ID))
//        {
//            rs = conn.createStatement().executeQuery("select NAME_ID,VALUE_ID from RBB_TAGS_IN_TAGSETS WHERE TAGSET_ID = "
//                + TAGSET_ID);
//
//            conn.createStatement().execute("delete from RBB_TAGS_IN_TAGSETS where TAGSET_ID = "
//                + TAGSET_ID);
//
//            H2SRBB.deleteStringIfNotInUse(conn, TAGSET_ID);
//
//            while (rs.next())
//            {
//                H2SRBB.deleteStringIfNotInUse(conn, rs.getLong(1));
//                H2SRBB.deleteStringIfNotInUse(conn, rs.getLong(2));
//            }
//        }
//
        return 1;
    }

    /**
     * Start or add to a sequence of Events that share a set of tags but differ in other tags.
     *
     * <pre>
     * sequence(db, time, idTags, infoTags)
     * Starts an event (starting at 'time' and ending at H2SRBB.maxDouble) with the union of idTags and infoTags, unless
     * an event with that tagset (or a superset of that tagset) already existed at the given time, in which case its end time is set to 'time'
     * Any/all other events matching idTags are ended at 'time'
     *
     * The premise of this function is a sequence of related Events.
     * The sequence is identified uniquely by 'idTags' and dynamic information is
     * given by 'infoTags'.
     *
     * For example, the following events record the city Bob is in over time:
     *
     * id=sequence(db, 10.0, "name=Bob", "city=ABQ");
     * id=sequence(db, 20.0, "name=Bob", "city=ABQ");
     * id=sequence(db, 30.0, "name=Bob", "city=LA");
     * id=sequence(db, 40.0, "name=Bob", "city=SLC");
     * id=sequence(db, 50.0, "name=Bob", "city=ABQ");
     * setEndByID(rbb.db(), id, 100.0);
     *
     * This creates the following events (note the repetition of ABQ is omitted)
     * get(db, "name=Bob", null, null, null);
     * ID       START_TIME  END_TIME    TAGS
     * 1        10.0        30.0        city=ABQ,name=Bob
     * 2        30.0        40.0        city=LA,name=Bob
     * 3        40.0        50.0        city=SLC,name=Bob
     * 4        50.0        100.0       city=ABQ,name=Bob
     *
     * Now we can ask, where was Bob at t=33?
     * get(db, "name=Bob", 33.0, 33.0, null);
     * ID       START_TIME END_TIME  TAGS
     * 2        30.0       40.0      city=LA,name=Bob
     * 
     * when was Bob in ABQ?
     * get(db, "name=Bob,city=ABQ", null, null, null);
     * ID       START_TIME END_TIME  TAGS
     * 1        10.0       30.0      city=ABQ,name=Bob
     * 4        50.0       100.0     city=ABQ,name=Bob
     * 
     * </pre>
     * 
     */
    public static Long sequence(Connection conn, Double time, String idTags, String infoTags) throws SQLException
    {
        Long result = -1L;
        ResultSet rs = find(conn, byTags(idTags), byTime(time, time));

        Tagset allTags = new Tagset(idTags);
        if(infoTags != null)
            allTags.add(new Tagset(infoTags));

        int numMatch = 0;

        while(rs.next()) {
            final Tagset curTags = new Tagset(rs.getString("TAGS"));
            if(allTags.isSubsetOf(curTags) && ++numMatch == 1) {
                result = rs.getLong("ID");
            }
            else if(time != null) {
                // this will never extend the end time of an event because of the
                // 'time' arguments passed to 'get' above.
                setEndByID(conn, rs.getLong("ID"), time);
           }
        }
        if(numMatch == 0) {
            result = create(conn, time, H2SRBB.maxDouble(), allTags.toString());
        }
        return result;
    }

    /**
     * throws exception if eventID is not an eventID
     * @param conn
     * @param eventID
     * @return
     */
    private static long getTagsetID(Connection conn, long eventID) throws SQLException {
        java.sql.PreparedStatement ps = conn.prepareStatement("SELECT TAGSET_ID  from RBB_EVENTS where ID = ?");
        ps.setLong(1, eventID);
        ResultSet rs = ps.executeQuery();
        if(!rs.next()) {
            throw new SQLException("getTagsetID error - "+eventID+" is not a valid event id");
        }
        return rs.getLong(1);
    }

    /**
     * Retrieve the tagset for the event.
     * Raises a SQL Exception for invalid ID.
     *
     * @param conn
     * @param eventID
     * @return
     * @throws SQLException
     */
    public static String getTagsByID(Connection conn, long eventID) throws SQLException
    {
        return H2STagset.fromID(conn, getTagsetID(conn, eventID));
    }

    /**
     *
     * Has the effect of calling Tagset.set(newTags) on the specified event.
     * Note that (following the semantics of Tagset.set(newTags), this does NOT
     * remove any previous name/value pairs for which no new value is specified!
     *
     * @param conn
     * @param eventID
     * @param newtags
     * @throws SQLException
     */
    public static void setTagsByID(Connection conn,
        long eventID,
        String newtags_)
        throws SQLException
    {
        Tagset newtags = new Tagset(newtags_);
        if(newtags.containsValue(null))
            throw new SQLException("Error: setTags got a null tag value which is not allowed.  (Did you want removeTags instead?)");

        synchronized(conn) {
            Tagset oldTags = new Tagset(getTagsByID(conn, eventID));
            oldTags.set(newtags);
            PreparedStatement ps = conn.prepareStatement("update RBB_EVENTS set TAGSET_ID=(select RBB_TAGSET_TO_ID(?)) where id = ?");
            ps.setObject(1, oldTags.toString());
            ps.setLong(2, eventID);
            ps.execute();
        }
    }

    /**
     *
     * Has the effect of calling Tagset.set(newTags) on all events matching the filterTags.
     * Note that (following the semantics of Tagset.set(newTags), this does NOT
     * remove any previous name/value pairs for which no new value is specified!
     *
     * @param conn
     * @param eventID
     * @param newtags
     * @throws SQLException
     */
    public static void setTags(Connection conn, String filterTags, String newtags) throws SQLException {
        ResultSet rs = find(conn, byTags(filterTags));
        while(rs.next()) {
            setTagsByID(conn, rs.getLong(1), newtags);
        }
    }

    /**
     * Call deleteData from command-line interface
     */
    public static void deleteDataCLI(String[] args) throws SQLException {
        if(args.length != 3) {
            System.err.println("deleteAttachments <RBB_URL> <schema> <eventID>: delete attached data.  schema and eventID may be null.");
            System.exit(1);
        }
        RBB rbb = RBB.connect(args[0]);
        H2SEvent.deleteData(
                rbb.db(),
                args[1].equals("null") ? null : args[1],
                args[2].equals("null") ? null : Long.parseLong(args[2]));
        rbb.disconnect();
    }

    /**
     * Given the ID of an event, removes all name/value pairs in removeTags.
     * Values in removeTags may be null, in which case all name/value pairs with matching name are removed.
     * It is not an error for removeTags to specify names or name/value pairs that were not in the tagset for the event; these are silently ignored.
     *
     * @param conn
     * @param eventID
     * @param removeTags
     * @throws SQLException
     */
   public static void removeTagsByID(Connection conn, long eventID, String removeTags) throws SQLException
    {
        Tagset tags = new Tagset(getTagsByID(conn, eventID));
        final int oldNumTags = tags.getNumTags();
        Tagset remove = new Tagset(removeTags);

        for(String name : remove.getNames())
        for(String value : remove.getValues(name)) {
            if(value == null)
                tags.remove(name.toString());
            else
                tags.remove(name.toString(), value.toString());
        }
        if(tags.getNumTags() == oldNumTags)
            return; // nothing was changed.

        java.sql.PreparedStatement ps = conn.prepareStatement("update RBB_EVENTS set TAGSET_ID=(select RBB_TAGSET_TO_ID(?)) where id = ?");
        ps.setObject(1, tags.toString());
        ps.setLong(2, eventID);
        ps.execute();

    }

    /**
     * Add additional tags to an event specified by ID
     * @param conn
     * @param eventID: The event to which the tags will be added.
     * @param addTagsByID The tags to add, e.g. "tag1=value1,tag2=value2"
     */
    public static void addTagsByID(Connection conn, long eventID, String addTags) throws SQLException
    {
        Tagset t = new Tagset(getTagsByID(conn, eventID));
        Tagset newTags = new Tagset(addTags);
        if(!newTags.isSubsetOf(t)) {
            t.add(newTags);
            setTagsByID(conn, eventID, t.toString());
        }
    }

    /**
     * Add tags to all the Events whose current tagset matches filterTags.
     * @param conn
     * @param filterTags
     * @param additionalTags
     * @throws SQLException
     */
    public static void addTags(Connection conn, String filterTags, String additionalTags) throws SQLException
    {
        ResultSet rs = find(conn, byTags(filterTags));
        while(rs.next()) {
            addTagsByID(conn, rs.getLong(1), additionalTags);
        }
    }
   /**
     * has the effect of invoking removeTagsByID on all Events whose tags match the filterTags.
    *  see removeTagsByID for additional documentation.
    *
     * @param conn
     * @param filterTags
     * @param additionalTags
     * @throws SQLException
     */
    public static void removeTags(Connection conn, String filter, String removeTags) throws SQLException
    {
        ResultSet rs = find(conn, RBBFilter.fromString(filter));
        while(rs.next()) {
            removeTagsByID(conn, rs.getLong(1), removeTags);
        }
    }

    /**
     * Remove tags from matching Events using command-line interface.
     */
    public static void removeTagsCLI(String[] args) throws SQLException {
        if(args.length != 3)
            throw new SQLException("Usage: removeTags <RBB_URL> <filterTags> <tagName[=tagValue]...>");
        RBB rbb = RBB.connect(args[0]);
        H2SEvent.removeTags(rbb.db(), args[1], args[2]);
        rbb.disconnect();
    }

    /**
     * Retrieve the START_TIME from RBB_EVENTS for the specified event.  Executes a query.
     * @param conn
     * @param id
     * @param time
     * @throws SQLException
     */
    public static double getStartByID(Connection conn,
        long id)
        throws SQLException
    {
        String q = "select START_TIME from RBB_EVENTS where ID = " + id + ";";
        ResultSet rs = conn.createStatement().executeQuery(q);
        rs.next();
        return rs.getDouble(1);
    }

    /**
     * Retrieve the END_TIME from RBB_EVENTS for the specified event.  Executes a query.
     * @param conn
     * @param id
     * @param time
     * @throws SQLException
     */
    public static double getEndByID(Connection conn, long id)
        throws SQLException
    {
        String q = "select END_TIME from RBB_EVENTS where ID = " + id + ";";
        ResultSet rs = conn.createStatement().executeQuery(q);
        rs.next();
        return rs.getDouble(1);
    }

    /**
     * Set the START_TIME in RBB_EVENTS for the specified event.  Executes a query.
     * @param conn
     * @param id
     * @param time
     * @throws SQLException
     */
    public static void setStartByID(Connection conn,
        long id,
        double time)
        throws SQLException
    {
        setByID(conn, id, time, null, null);
    }

    /**
     * Set the END_TIME in RBB_EVENTS for the specified event.  Executes a query.
     * @param conn
     * @param id
     * @param time
     * @throws SQLException
     */
    public static void setEndByID(Connection conn,
        long id,
        double time)
        throws SQLException
    {
        setByID(conn, id, null, time, null);
    }
    
    
    /**
     * Set any/all attributes of an existing event, specified by ID.
     * Any null parameters are ignored.
     * Does nothing if startTime, endTime, and tagset are all NULL.
     * Otherwise, raises an exception if the ID does not specify an existing event.
     *<p>
     * If a tagset is specified, it completely replaces any previous tagset.
     * This is different than setTagsByID, which only replaces the values of
     * tags specified in the new tagset, leaving other tags as they were.
     *<p>
     * If startTime is not null, any attachments with non-null
     * timestamps are deleted if they are before the new start time.
     * Likewise for end-time.
     *
     * @param conn
     * @param id
     * @param startTime
     * @param endTime
     * @param tagset
     * @throws SQLException
     */
    public static void setByID(Connection conn,
        long id,
        Double startTime,
        Double endTime,
        Object tagset)
        throws SQLException
    {
        if(startTime==null && endTime==null && tagset==null)
            return;

        synchronized(conn) {
            PreparedStatementCache.Query q = PreparedStatementCache.startQuery(conn);

            PreparedStatementCache.Delim csv = PreparedStatementCache.Delim.CSV();

            q.add("update RBB_EVENTS set");

            if(startTime != null)
                q.addAlt(csv.get()+" START_TIME=", startTime);

            if(endTime != null)
                q.addAlt(csv.get()+" END_TIME=", endTime);

            if(tagset != null)
                q.addAlt(csv.get()+" TAGSET_ID=(select RBB_TAGSET_TO_ID(", tagset, "))");

            q.addAlt(" where ID = ", id);

            final int n = q.getPreparedStatement().executeUpdate();

            if(n != 1)
                throw new SQLException("H2SEvent.setByID error: update count of 1 expected, got " + n);

            // now delete attachments outside the new start / end times
            if(startTime != null || endTime != null) {
                q = PreparedStatementCache.startQuery(conn);
                q.addAlt("select SCHEMA_NAME, TABLE_NAME from RBB_EVENT_DATA where EVENT_ID=", id);
                ResultSet rsAttachments = q.getPreparedStatement().executeQuery();

                while(rsAttachments.next()) { // for each table in which the source event has attached data...
                    final String schemaTable =  "\"" + rsAttachments.getString("SCHEMA_NAME") + "\".\"" + rsAttachments.getString("TABLE_NAME") + "\"";

                    PreparedStatementCache.Query psDelete = PreparedStatementCache.startQuery(conn);
                    psDelete.addAlt("delete from "+schemaTable+" where EVENT_ID = ", id, " and ");

                    if(startTime != null && endTime != null)
                        psDelete.addAlt("TIME < ", startTime, " or TIME > ", endTime);
                    else if(startTime != null)
                        psDelete.addAlt("TIME < ", startTime);
                    else
                        psDelete.addAlt("TIME > ", endTime);

                    psDelete.getPreparedStatement().execute();
                }
                rsAttachments.close();
            }
        }
    }

    /***
     * Set the end time for all events matching the filter.
     * <p>
     * The filter is a string, so it can be a tagset, or any other Filter string.
     */
    public static int setEnd(Connection conn, String filter, double time) throws SQLException
    {
        int n = 0;
        ResultSet rs = find(conn, RBBFilter.fromString(filter));
        while(rs.next()) {
            setEndByID(conn, rs.getLong("ID"), time);
            ++n;
        }
        return n;
    }

    /***
     * <pre>
     * Find Events that overlap in time
     * filterTagsArray specifies sets of events.  It is an array of Tagset arrays
     *
     * 'skip', if not null, is a 2d array of Longs.  Each row is a set of Event IDs
     * that will be excluded from the result (in every permutation)
     *
     * In java:
     *
     *       ResultSet rs = H2SEvent.findConcurrent(rbb.db(),
     *           new Object[]{"test=x", "test=y"}, null, null,null,null);
     *
     * or
     *
     *       ResultSet rs = H2SEvent.findConcurrent(rbb.db(),
     *           new Object[]{"test=x", "test=y;test=z"}, null, null,null,null);
     *
     * or in SQL:
     *
     *   ResultSet rs = rbb.db().createStatement().executeQuery(
     * "call rbb_concurrent_events((('set','a'),('set','b'), ('set','c')), ((1,2),(3,4)), 2.5, 100, null)");
     *
     * example return table:
     * START_TIME  	END_TIME  	IDS
     * 1.15         6.35        (1, 18)
     * 1.75         4.05        (1, 5)
     *
     * This function finds all combinations of Events - one from the first set, one from the second set, and so on - that overlap in time.
     * Also returns the time intervals for which they overlap (which overlap with START and END but may extend beyond them).
     * A given Event occurs at most once in any combination of Events, even if it matches more than one tagset.
     * Only one permutation of each combination is returned, even if there are different permutations that could match.
     * START and END may be null, in which case the query is not constrained by time.
     *
     * Any tag with an empty-string value inherits a value for that tag from its
     * nearest predecessor in the parameter list that had a value for that tag.
     * If there is no such predecessor, the tag must be present but can have any value.
     *
     * If 'schema' is non-null, the search will be limited to Events with attached data in that schema.
     *
     * </pre>
     *
     *
     * */
    public static ResultSet findConcurrent(
        Connection conn,
        Object[] filterArray, // this is an array of String, which are tagsets or RBBFilters or either in the String form.
        Object[] skip,
        Double start,
        Double end,
        String timeCoordinate,
        String schema)
        throws SQLException
    {
        org.h2.tools.SimpleResultSet result = new org.h2.tools.SimpleResultSet();
        result.addColumn("START_TIME", java.sql.Types.DOUBLE, 20, 0);
        result.addColumn("END_TIME", java.sql.Types.DOUBLE, 20, 0);
        result.addColumn("IDS", java.sql.Types.ARRAY, 20, 0);

        if (conn.getMetaData().getURL().equals("jdbc:columnlist:connection")) {
            return result;
        }

//        Tagset[][] filterTags = new Tagset[filterTagsArray.length][];
//        for(int i = 0; i < filterTags.length; ++i) {
//            filterTags[i] = Tagset.toTagsets(filterTagsArray[i]);
//            // System.err.println("findConcurrent: " + filterTags[i].length + " alternates for column " + i);
//        }

        RBBFilter[] filters = new RBBFilter[filterArray.length];
        for(int i = 0; i < filters.length; ++i)
            filters[i] = RBBFilter.fromString(filterArray[i].toString());

//        Tagset contextTags = new Tagset();

//        gov.sandia.rbb.Tagset outputTimeCoordinate = null;
//        if (timeCoordinate != null)
//            outputTimeCoordinate = new Tagset(timeCoordinate);

        // copy 2d array skip into set of sets skipSet.
        HashSet<Set<Long>> skipSet = new HashSet<Set<Long>>();
        if(skip != null) {
            for(Object a : skip) {
                Set<Long> s = new HashSet<Long>();
                for(Object i : (Object[])a)
                    s.add((Long)i);
                skipSet.add(s);
            }
        }

        RBBFilter rbbFind = new RBBFilter(byTime(start,end), bySchema(schema));
        if(timeCoordinate != null)
            rbbFind.also(withTimeCoordinate(timeCoordinate));

        findConcurrentHelper(conn, rbbFind, 0,
            filters,
            new Tagset(),
            new Long[filters.length],
            result,
            skipSet);

        return result;
    }
// byTime(start, end), withTimeCoordinate(outputTimeCoordinate), byData(schema)
    /**
     * This is a recursive function used by findConcurrent.
     * @param conn
     * @param rbbFind specifies the start time, end time, time coordinate, and can limit the data schema (to timeseries, normally).  However, it doesn't specify tags, since those are different for each column.
     * @param depth is the current level of recursion
     * @param filterTags array is as specified by the user (filter tags for each level)
     * @param contextTags is the union of all filterTags from depth 0 to 'depth'
     * @param ids array of the events currently in the stack
     * @param result
     * @param permutations
     * @throws SQLException
     */
    private static void findConcurrentHelper(Connection conn,
        RBBFilter rbbFind,
        int depth,
        RBBFilter[] filters,
        Tagset contextTags,
        Long[] ids,
        org.h2.tools.SimpleResultSet result,
        Set<Set<Long>> permutations)
        throws SQLException
    {
        for(RBBFilter filter : filters[depth].getTagDisjunction())
        {
            // bind any unbound tags in the filter tagset
            Tagset t = filter.tags[0]; // has exactly 1 element because it was produced by getTagDisjunction
            Set<String> tagNames = new HashSet<String>(); // make a copy of tagnames so we can iterate over and add to it without concurrentModificationException.
            tagNames.addAll(t.getNames());
            for(String name : tagNames)
                if(t.contains(name, "")) {
                    String newValue = contextTags.getValue(name);
                    if(newValue == null)
                        throw new SQLException("Error: Tagset "+t+" has an empty-valued tag, but no tagsets to its left specify a value for that tag");
                   t.set(name, newValue);
                }

            // debug message
//            System.err.print("findConcurrentHelper:");
//            for(int i = 0; i < depth; ++i)
//                System.err.print("\t");
//            System.err.println(filter);

            RESULTS:
            for(Event ev : Event.find(conn, rbbFind, filter))
            {
                // add tags from the event on the top of the stack to the context, making a new, augmented context for further recursion
                gov.sandia.rbb.Tagset newContextTags = contextTags.clone();
                newContextTags.set(ev.getTagset());

                // don't report that an Event is concurrent with itself.  Make sure each id occurs only once in the set of IDs
                for (int i = 0; i < depth; ++i)
                {
                    if (ids[i] == ev.getID())
                    {
                        // System.err.println("not matching Event " + id + " to itself");
                        continue RESULTS;
                    }
                }

                ids[depth] = ev.getID();

                //// check against finding different permutations of the same result.
                // make a set containing the IDs
                Set<Long> idSet = new HashSet<Long>();
                for (int i = 0; i <= depth; ++i)
                    idSet.add(ids[i]);

                // don't return different permutations of the same solution
                if (permutations.contains(idSet))
                {
                    // System.err.println("skipping permutation; already did it");
                    continue;
                }
                else
                {
                    // System.err.println("adding permutation of " + idSet.toString());
                    permutations.add(idSet);

                    // finding a huge number of permutations is often a sign of accidentally specifying
                    // an over-general tagset
                    if(permutations.size() >0 && permutations.size() % 10000 == 0)
                        System.err.println("findConcurrent: "+permutations.size()+" permutations so far...");
                }

                // get the start and end times of the current result from the ResultSet,
                // not only to avoid unnecessarily re-computing it, but because it already has time conversion (if any) performed.

                final double latestStart = rbbFind.start == null ? ev.getStart() : Math.max(rbbFind.start, ev.getStart()); // constrain start and and to this Event.
                final double earliestEnd = rbbFind.end == null ? ev.getEnd() : Math.min(rbbFind.end, ev.getEnd());

                if (depth + 1 == ids.length)
                { // we have a result
                    // the clone here is very important because addRow doesn't make a copy of the array...
                    // ids is used as a stack and will change over time, so without clone, every row of the
                    // resultSet has the same array of ids!
                    result.addRow(latestStart, earliestEnd, ids.clone());
                }
                else
                { // need to recurse
                    findConcurrentHelper(conn, 
                        new RBBFilter(rbbFind, byTime(latestStart, earliestEnd)),
                        depth + 1, filters,
                        newContextTags,
                        ids, 
                        result, permutations);
                }
            }
        }
    }

    /***
     * <pre>
     * Attach rows from a table to an event.
     *
     * This means:
     *
     * 1) When the event is deleted, the rows will be deleted.
     * 2) When data is inserted into the data table, any listeners to the event
     *    will receive a call to EventListener.eventDataAdded
     * 3) clonePersistent will clone attachments
     * 4) When the start or end time of the Event is changed, any attachments
     *    before the new start or after the new end will be deleted.
     *
     * Any data previously attached to the same event in the same schema is first removed.
     *
     * The schema and table must already exist.
     * The table must include columns: EVENT_ID BIGINT, TIME DOUBLE
     *   Rows in the TIME column are allowed to be null, which indicates the data
     *   is attached to the event at no particular time.
     *
     * If what you need is 'opaque' data associated with the Event,
     * see H2SBlob.attachBlob for creating the schema and table and linking it to an Event,
     * 
     *</pre>
     * @param eventID identifies the event that owns the data
     * @param schemaName identifies the schema where the table is found
     * @param dataTable identifies the table holding the data that is owned.
     * @param eventIDColumn is null if the Event owns the entire table.  Otherwise, IDColumn
     * is the name of the column in dataTable that contains the ID of the Event owning that row of data.
     *
     */
    public static void attachData(Connection conn, Long eventID, String schemaName, String dataTable) throws SQLException
    {
        deleteData(conn, schemaName, eventID);
        attachDataUnchecked(conn,eventID,schemaName,dataTable);
    }

    /**
     * Attaches data to an event without first removing any previous attachment to the same schema.
     * This can be used when attaching data to a newly created event for which there
     * cannot possibly be data attached.
     */
    static void attachDataUnchecked(Connection conn, Long eventID, String schemaName, String dataTable) throws SQLException
    {
        synchronized(conn) {
            PreparedStatementCache.Query q = PreparedStatementCache.startQuery(conn);
            q.add("insert into RBB_EVENT_DATA values ");
            q.addParamArray(eventID, schemaName, dataTable);
            q.getPreparedStatement().execute();

            PreparedStatementCache.Query trigger = PreparedStatementCache.startQuery(conn);
            // there are no parameters in this statement because I don't think you can specify
            // table names as parameters to stored procedures.  But for the common case
            // (repetitive creation of a trigger on a timeseries table) it will all be the same every time.
            trigger.add("CREATE TRIGGER if not exists RBB" + dataTable
                + "INS AFTER INSERT ON " + (schemaName==null ? "" : "\""+schemaName+"\".") + "\""+dataTable+"\""
                + " FOR EACH ROW CALL \"gov.sandia.rbb.impl.h2.H2EventDataTrigger\";");
            trigger.getPreparedStatement().execute();
        }
    }

    /**
     * Retrieve the names of data tables attached to the specified event in the specified schema.
     * If no such tables exist, an empty array (not null) is returned.
     * The returned table names do not include the schema name.
     */
    public static String[] attachedDataTables(Connection conn, long eventID, String schemaName) throws SQLException
    {
        synchronized(conn) {
            PreparedStatementCache.Query q = PreparedStatementCache.startQuery(conn);
            q.add("select TABLE_NAME from RBB_EVENT_DATA where EVENT_ID = ");
            q.addParam(eventID);
            q.add(" and SCHEMA_NAME = ");
            q.addParam(schemaName);
            ResultSet rs = q.getPreparedStatement().executeQuery();
            if(!rs.next()) {
                rs.close();
                return new String[]{};
            }
            ArrayList<String> results = new ArrayList<String>();
            results.add(rs.getString(1));
            while(rs.next())
                results.add(rs.getString(1));
            rs.close();
            return results.toArray(new String[]{});
        }
    }

    /*
     * Make a copy of the specified Event in the RBB, and return the new ID for it.
     * <p>
     * This also clones the attachments to the Event,
     * however it can only clone attached rows and not entire tables.
     * (This is because it has no way of knowing what the new table name should be)
     *
     */
    public static Long clonePersistent(Connection conn, Long sourceID, Double newStart, Double newEnd, String setTags) throws SQLException {
        synchronized(conn) {
            Event srcEvt = Event.getByID(conn, sourceID);

            Event newEvt = new Event(srcEvt);
            newEvt.isPersistent = false; // make this copy transient for the moment...

            if(newStart != null)
                newEvt.setStart(newStart);
            if(newEnd != null)
                newEvt.setEnd(newEnd);
            if(setTags != null)
                newEvt.getTagset().set(new Tagset(setTags));

            // make it persistent.
            newEvt = new Event(conn, newEvt.getStart(), newEvt.getEnd(), newEvt.getTagset());
            
            // clone attachments, such as timeseries data
            PreparedStatementCache.Query q = PreparedStatementCache.startQuery(conn);
            q.addAlt("select SCHEMA_NAME, TABLE_NAME from RBB_EVENT_DATA where EVENT_ID=", sourceID);
            ResultSet rsAttachments = q.getPreparedStatement().executeQuery();

            while(rsAttachments.next()) { // for each schema in which the source event has attached data...
                final String schemaTable =  "\"" + rsAttachments.getString("SCHEMA_NAME") + "\".\"" + rsAttachments.getString("TABLE_NAME") + "\"";
                attachDataUnchecked(conn, newEvt.getID(), rsAttachments.getString("SCHEMA_NAME"), rsAttachments.getString("TABLE_NAME"));
                PreparedStatementCache.Query psCloneData = PreparedStatementCache.startQuery(conn); // clone data in this table from existing event to new one.
                psCloneData.add("insert into "+schemaTable+" (select ");
                String[] dataTableCols = getColNames(conn, rsAttachments.getString("SCHEMA_NAME"), rsAttachments.getString("TABLE_NAME"));
                for(int i = 0; i < dataTableCols.length; ++i) {
                    if(i > 0)
                        psCloneData.add(",");
                    if(dataTableCols[i].equals("EVENT_ID"))
                        psCloneData.addParam(newEvt.getID());
                    else
                        psCloneData.add(dataTableCols[i]);
                }

                psCloneData.addAlt(" from "+schemaTable+" where EVENT_ID = ", sourceID);

                // time restriction for attached data.
                if(newStart != null || newEnd != null) {
                    psCloneData.add(" and (TIME is NULL or ");
                    if(newStart != null && newEnd != null)
                        psCloneData.addAlt("TIME between ", newStart, " and ", newEnd);
                    else if(newStart != null)
                        psCloneData.addAlt("TIME >= ", newStart);
                    else
                        psCloneData.addAlt("TIME <= ", newEnd);
                    psCloneData.add(" )");
                }

                psCloneData.add(");");
                // System.err.println(psCloneData.toString());
                psCloneData.getPreparedStatement().execute();
            }
            rsAttachments.close();

            return newEvt.getID();
        }
    }


    /*
     * Get names of the specified table, in order.
     */
    private static String[] getColNames(Connection conn, String schemaName, String tableName) throws SQLException {
        synchronized(conn) {
            ArrayList<String> result = new ArrayList<String>();
            PreparedStatementCache.Query ps = PreparedStatementCache.startQuery(conn); // get the names of the columns in the attached data table.
            ps.addAlt("select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA = ", schemaName, " and TABLE_NAME = ", tableName, " order by ORDINAL_POSITION;");
            ResultSet rs = ps.getPreparedStatement().executeQuery();
            while(rs.next()) {
                result.add(rs.getString(1));
            }
            rs.close();
            return result.toArray(new String[0]);
        }
    }
}
