/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.impl.h2.statics;

import gov.sandia.rbb.Event;
import gov.sandia.rbb.RBBFilter;
import static gov.sandia.rbb.RBBFilter.*;
import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.Timeseries;
import gov.sandia.rbb.Timeseries.Sample;
import gov.sandia.rbb.PreparedStatementCache;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import org.h2.tools.SimpleResultSet;

/**
 *
 * H2STimeseries contains static methods that are stored procedures in the H2 implementation of RBB Time Series
 * The create_rbb.sql script creates aliases for these so they can be called through SQL.
 * The public functions here only accept / return datatypes for which H2 has a SQL mapping.
 * Most functionality is placed here, with the RBB interface implementation (H2Timeseries) being a thin wrapper over them.
 *
 * @author rgabbot
 */
public class H2STimeseries
{

    /**
     * This is the name of the schema in which all timeseries tables are stored.
     */
    public static final String schemaName = "RBB_TIMESERIES";

    public static long start(Connection conn,
        int dim,
        double time,
        String tags)
        throws SQLException
    {
        // must create the ID for the new event *last*, then add it to the
        // table only after creating and attaching its rows table.
        // This is so event listeners interested in events affecting
        // this schema will receive notification when the event is added.
        final long id = H2SRBB.nextID(conn);

        //// create a table for timeseries rows

        final String tableName = "TF" + dim; // TF2 = Timeseries Floats 2D

        PreparedStatementCache.Query createTable = PreparedStatementCache.startQuery(conn);
        createTable.add("CREATE SCHEMA if not exists ",schemaName,";");
        createTable.add("CREATE TABLE if not exists ",schemaName,".",tableName,"(EVENT_ID BIGINT, TIME DOUBLE, PRIMARY KEY (EVENT_ID,TIME)");
        for (int i = 1; i <= dim; ++i)
            createTable.add(",C",Integer.toString(i)," REAL");
        createTable.add(");");
        createTable.getPreparedStatement().execute();

        // the "unsafe" version of attach data doesn't remove any previously attached
        // data first.  But since this is a brand new event, there cannot be any
        // previously attached data.
        H2SEvent.attachDataUnchecked(conn, id, schemaName, tableName);

        return H2SEvent.create(conn, id, time, H2SRBB.maxDouble(), tags);
    }

    /*
     * Create and populate a timeseries.
     */
    public static long create(Connection conn, int dim, double start, double end, String tags, Object[] times, Object[] samples) throws SQLException {
        long id = start(conn, dim, start, tags);
        addSamplesByID(conn, id, times, samples, null, end);
        return id;
    }


    /*
     * This function is used to create and maintain a timeseries with unique tagset
     * with timeseries values that may be reset at any time.
     * <p>
     * If a timeseries with the specified tags already exists, its existing samples are
     * removed and the new samples are added, and the start/end/ times and dim are updated,
     * and the ID is returned.
     * <p>
     * If a timeseries with the specified tags did not already exist, it is
     * created with the specified samples, and its ID returned.
     * <p>
     * So long as the timeseries with the specified tags is manipulated only through
     * this function, any query will see exactly one existing timeseries and set of samples.
     * <p>
     * If multiple timeseries already existed with the specified tagset, it is not
     * defined which will be updated.  This cannot happen if only this function is
     * used to modify the timeseries.
     */
//    public static long updateOrCreate(Connection conn, int dim, double start, double end, String tags, Object[] times, Object[] samples) throws SQLException {
//        ResultSet rs = H2SEvent.find(conn, byTags(tags));
//
//
//        H2SEvent.find(conn, filterTags, start, end, IDs, timeCoordinate, schemaName);
//
//        Timeseries[] prev = findWithoutSamples(conn, byTags(getTagset()));
//        if(prev.length > 0)
//            setEqual(prev);
//        else
//            persist(conn);
//    }

    private static class NoSuchTimeseries extends SQLException {
        NoSuchTimeseries(String s) { super(s); }
    }

    private static String getTableName(Connection conn, long id) throws SQLException {
        String[] tableNames = H2SEvent.attachedDataTables(conn, id, schemaName);
        if(tableNames.length == 0)
            throw new NoSuchTimeseries("H2STimeseries: no timeseries has been linked to event " + id);
        if(tableNames.length > 1)
            throw new SQLException("H2STimeseries: multiple timeseries have been linked to event " + id + ".  This is not supported");
        return tableNames[0];
    }

    public static Double getTimeFromRow(Object[] obs) {
        final int i = 1; // this is the 0-based index of the TIME column in the CREATE TABLE... statement above.
        if(obs[i] instanceof Double)
            return (Double) obs[i];
        else
            return Double.parseDouble(obs[i].toString());

    }

    /*
     * From an object array representing a row of a timeseries table,
     * get a Float[] of the timeseries sample data.
     */
    public static Float[] getSampleFromRow(Object[] obs) {
        final int offset = 2; //  because first column in a timeseries table is ID, second is TIME.  The rest are data.
        Float[] x = new Float[obs.length-offset];
        for(int i = 0; i < x.length; ++i) {
        if(obs[i] instanceof Float)
            x[i] = (Float) obs[i+offset];
        else
            x[i] = Float.parseFloat(obs[i+offset].toString());
        }
        return x;
    }

    /**
     * add a new observation to a timeseries
     * The rows is passed as a list.  The length of the list must equal the dimension of the timeseries.  The syntax for calling this from SQL is:
     * call rbb_add_to_timeseries(1, 9.63, (3.1, 2.4));
     *
     * if minTimeDelta is not null, each sample won't be added unless it's at least that much
     * after the previous sample.
     *
     * @param conn
     * @param id
     * @param time
     * @param rows
     * @throws SQLException
     */
    public static void addSampleByID(Connection conn,
        long id,
        double time,
        Object[] data,
        Double minTimeDelta,
        Double newEndTime)
        throws SQLException
    {
        addSamplesByID(conn, id, new Object[]{time}, new Object[]{data}, minTimeDelta, newEndTime);

    }

    /**
     * Add samples to a timeseries specified by ID.
     *
     * @param conn
     * @param id: id of timeseries for additions.
     * @param time: times of samples
     * @param data: a 2d array of samples.  data[i] is the ith sample, data[i][j] is the jth dimension of the sample.
     * @param minTimeDelta: drop samples less than this long after the previous one.
     * @param newEndTime: update the end time of the timeseries - otherwise is not modified.
     * @throws SQLException
     */
    public static void addSamplesByID(Connection conn,
        long id,
        Object[] time,
        Object[] data,
        Double minTimeDelta,
        Double newEndTime)
        throws SQLException
    {
        final String tableName = getTableName(conn, id);
        Double lastTime = null;
        int numBatch = 0;

        if(time.length != data.length)
            throw new SQLException("H2STimeseries.addSamplesByID error: the number of times and number of data samples do not match!");

        if(minTimeDelta != null)
            lastTime = lastTime(conn, tableName, id);

        if(data.length != 0) {
            if(!(data[0] instanceof Object[]))
                throw new SQLException("H2STimeseries.addSamplesByID error: row 0 of the data is not an array!");
            Object[] dataRow = (Object[]) data[0];
            PreparedStatementCache.Query q = PreparedStatementCache.startQuery(conn);
            q.addAlt("insert into "+schemaName+"."+tableName+" values (", id, ",", time[0]);
            for (int i = 0; i < dataRow.length; ++i)
                q.addAlt(",", dataRow[i]);
            q.add(");");
            PreparedStatement ps = q.getPreparedStatement();

            for(int i = 0; i < data.length; ++i) {
                final double t = (Double) time[i];

                if(minTimeDelta != null && lastTime != null && t-lastTime < minTimeDelta)
                    continue;

                if(!(data[i] instanceof Object[]))
                    throw new SQLException("H2STimeseries.addSamplesByID error: row "+i+" of the data is not an array!");
                dataRow = (Object[]) data[i];
                ps.setObject(2, t);
                for(int j = 0; j < dataRow.length; ++j)
                    ps.setObject(3+j, dataRow[j]); // 3 because setObject is 1-based, and [1] is timeseries ID and [2] is time.
                ps.addBatch();
                lastTime = t;
                ++numBatch;
            }
            ps.executeBatch();
        }

        // System.err.println(sw.toString());

        if(newEndTime != null && numBatch > 0) { // don't update the end time if there were no samples or none exceeded minTimeDelta, since the purpose of minTimeDelta is to reduce constant writes when samples are overly dense.
          H2SEvent.setEndByID(conn, id, newEndTime);
        }
    }

    /**
     * add a new observation to all timeseries that match a tagset
     * The rows is passed as a list.  The length of the list must equal the dimension of the timeseries.  The syntax for calling this from SQL is:
     * call rbb_add_to_timeseries(1, 9.63, (3.1, 2.4), null);
     *
     * If there were no sequences with the specified tagset, one is started.
     *
     * @param conn
     * @param tagset
     * @param time
     * @param rows
     * @param newEndTime also set the end time of the timeseries to this value, unless null.
     * @throws SQLException
     */
    public static int addSample(Connection conn,
        String tagset,
        double time,
        Object[] data,
        Double minTimeDelta,
        Double newEndTime)
        throws SQLException
    {
        int n = 0;
        ResultSet rs = H2SEvent.find(conn, byTags(tagset));
        while(rs.next())
        {
            addSampleByID(conn, rs.getLong("ID"), time, data, minTimeDelta, newEndTime);
            ++n;
        }
        if(n == 0)
        {
            final long id = start(conn, data.length, time, tagset);
            addSampleByID(conn, id, time, data, minTimeDelta, newEndTime);
            ++n;
        }
        return n;
    }

    /**
     * Delete samples from a timeseries between the specified times.
     * The start and/or end may be null, which is equivalent to -infinity and +infinity respectively.
     */
//    public static int deleteSamplesByID(Connection conn, long id, Double start, Double end) throws SQLException {
//        synchronized(conn) {
//            PreparedStatementCache.Query q = PreparedStatementCache.startQuery(conn);
//            q.addAlt("delete from "+schemaName+"."+ getTableName(conn,id) + " where EVENT_ID=", id);
//            if(start != null)
//                q.addAlt(" and TIME >= ", start);
//            if(end != null)
//                q.addAlt(" and TIME <= ", end);
//            return q.getPreparedStatement().executeUpdate();
//        }
//    }

    /**
     * find the times of the last n updates in the specified timeseries, in descending order
     */
    private static Double lastTime(Connection conn, String tableName, long id)
        throws SQLException
    {
            synchronized(conn) {
            PreparedStatementCache.Query q = PreparedStatementCache.startQuery(conn);

            q.addAlt("select max(time) from "+schemaName+"."+tableName+" where EVENT_ID=", id);
            ResultSet rs = q.getPreparedStatement().executeQuery();
            if(!rs.next())
                return null;
            return rs.getDouble(1);
        }
    }

    /**
     * Returns the dimensionality of a Timeseries.
     * Returns null if the ID is not a timeseries ID.
     * Throws SQLException if there is error.
     *
     * @param conn
     * @param id
     * @return
     * @throws SQLException
     */
    public static Integer getDim(Connection conn, long id) throws SQLException
    {
        PreparedStatementCache.Query q = PreparedStatementCache.startQuery(conn);
        q.addAlt("select count(*) from information_schema.columns C join rbb_event_data E on EVENT_ID=",id," and C.table_name=E.table_name and C.table_schema=",schemaName," and E.SCHEMA_NAME=",schemaName,";");
        final ResultSet rs = q.getPreparedStatement().executeQuery();
        rs.next();
        Integer n = rs.getInt(1)-2; // -2 because first col is ID and second is TIME; the rest are data columns.
        rs.close();
        if(n<0)
            n = null;
        return n;
    }

    /**
     * Return the dimensionality of specified Timeseries IDs.
     * The resulting table has columns: EVENT_ID | DIM
     * Invalid Timeseries IDs (e.g. Events with no Timeseries data) are not in the result.
     *<p>
     * If calling from Java, you may want to use Timeseries.promoteEvents instead.
     *<p>
     * Note: If the Connection is being accessed from multiple threads, the caller
     * should synchronize on the Connection from before calling this until after 
     * the resultSet is closed.
     *
     * @param conn
     * @param ids: passed as Object[] because H2 cannot send a Long[]
     * @return
     * @throws SQLException
     */
    public static ResultSet getDims(Connection conn, Object[] ids) throws SQLException
    {
        PreparedStatementCache.Query q = PreparedStatementCache.startQuery(conn);
        q.add("select EVENT_ID, (select count(*) from information_schema.columns C where C.table_name=D.table_name and C.table_schema='RBB_TIMESERIES')-2 as DIM from rbb_event_data D where D.EVENT_ID in ");
        q.addParamArray(ids);
        return q.getPreparedStatement().executeQuery();
    }

    public static int getNumObservations(Connection conn, long id) throws SQLException
    {
        PreparedStatementCache.Query q = PreparedStatementCache.startQuery(conn);
        q.add("SELECT count(*) FROM ",schemaName,".",getTableName(conn,id)," where EVENT_ID=");
        q.addParam(id);
        final ResultSet rs = q.getPreparedStatement().executeQuery();
        rs.next();
        int n = rs.getInt(1);
        rs.close();
        return n;
    }

   /**
     * retrieve the value of the specified timeSeries, as of the last update at or before the specified time.
     * If there are no samples at or before the time, return the first value *after* the specified time.
     * If the timeSeries is empty, throw an exception.
     * @param conn
     * @param id
     * @param time
     * @param timeCoordinate
     * @return
     * @throws SQLException
     */
    public static Float[] valuePrev(Connection conn,
        long id,
        double time,
        String timeCoordinate)
        throws SQLException
    {
        ResultSet rs = getSamples(conn, id, time, time, 1, 1, timeCoordinate, null);
        if(!rs.next())
            return null;
        Sample c1 = new Sample(rs);
        if(!rs.next())
            return c1.getValue();
        Sample c2 = new Sample(rs);
        if(c2.getTime() == time)
            return c2.getValue();
        else
            return c1.getValue();
    }

    /**
     * retrieve the value using the interpolation type specified by 'interpolate'.
     * if 'interpolate' is null, use the 'interpolate=' tag on the timeSeries.
     * Otherwise, interpolates to interpolate=linear.
     * @param time
     * @param timeCoodinate
     * @return Returns the values as a Float[].  (Returning the primitive type, float[], is not supported in H2).
     * @throws SQLException
     */
    public static Float[] value(Connection conn,
        long id,
        double time,
        String timeCoordinate,
        String interpolate)
        throws SQLException
    {
        if (interpolate == null)
        {
            Tagset tags = new Tagset(H2SEvent.getTagsByID(conn, id));
            interpolate = tags.getValue("interpolate");
        }
        if (interpolate == null || interpolate.equals("linear"))
        {
            return valueLinear(conn, id, time, timeCoordinate);
        }
        else if (interpolate.equals("prev"))
        {
            return valuePrev(conn, id, time, timeCoordinate);
        }
        else
        {
            throw new SQLException("tried to apply unknown interpolation type " + interpolate);
        }
    }

    /**
     * Return rows of the table rows for this timeseries.
     * iStart is index of first value retrieved - 0 if null.
     * n is maximum number to return - all if null.
     */
    public static ResultSet getSamples(Connection conn, long eventID, Integer iStart, Integer n) throws SQLException
    {
        PreparedStatementCache.Query q = PreparedStatementCache.startQuery(conn);
        q.add("select TIME");
        final int dim = getDim(conn, eventID);
        for(int i=0; i < dim; ++i)
            q.add(",C"+(i+1));
        q.add(" from ", schemaName, ".", getTableName(conn,eventID), " where EVENT_ID=");
        q.addParam(Long.toString(eventID));

        // in H2, you can only specify an OFFSET if you've specified a LIMIT
        if(n==null && iStart != null)
            n = getNumObservations(conn, eventID)-iStart;

        if(n != null)
            q.addAlt(" limit ", n);

        if(iStart != null)
            q.addAlt(" offset ", iStart);

        return q.getPreparedStatement().executeQuery();
    }

    final public static String RBB_EMPTY_TIMESERIES = "Empty RBB Timeseries";

    public static ResultSet resampleValues(Connection conn,
        Object[] IDs,
        Double start,
        Double timestep,
        Integer n,
        String timeCoordinate)
        throws SQLException
    {
        Double[] times = new Double[n];
        for(int i = 0; i < n; ++i)
            times[i] = start + timestep * i;
        return resampleValues(conn, IDs, times, timeCoordinate);
    }


    /**
     * Return linearly interpolated values of the specified timeseries (specified by IDs) at the specified times.
     *<p>
     * Any values previous to the timeseries start or after the end are NULL.
     *<p>
     * Any values at or after the timeseries start but before the first sample are extrapolated from the first two samples,
     * unless there's only one sample in which case that value is used.  Likewise for values before or at the end but after the last sample.
     *<p>
     * NULL values generated for an ID that corresponds to an Event that is not a Timeseries.
     *<p>
     * If times==null, the sample times of the first timeseries (IDs_[0]) are used, limited to the times at which all the Events exist.
     * In this case the first ID cannot be an event, it must be a timeseries.
     *<p>
     * If this operation fails because one of the timeseries identified by IDs_ is empty,
     * then the SQL state of the raised exception will be RBB_EMPTY_TIMESERIES.
     *
     */
    public static ResultSet resampleValues(Connection conn, Object[] IDs_, Double[] times, String timeCoordinate) throws SQLException
    {
        RBBFilter f = byID(H2SRBB.makeLongs(IDs_));
        if(timeCoordinate != null)
            f.also(withTimeCoordinate(new Tagset(timeCoordinate)));

        return resampleValues(conn, f, times);
    }

    public static ResultSet resampleValues(Connection conn, RBBFilter filter, Double[] times) throws SQLException {
        SimpleResultSet result = new SimpleResultSet();

        Event[] events = Event.getByIDs(conn, filter.IDs, filter);

        result.addColumn("TIME", java.sql.Types.DOUBLE, 5, 0);

        for(int i = 0; i < events.length; ++i)
            result.addColumn("C" + events[i].getID(), java.sql.Types.ARRAY, 5, 0);

        if (conn.getMetaData().getURL().equals("jdbc:columnlist:connection"))
            return result;

        if(times == null) {
            // find time bound of the set.
            Double start0 = Double.NEGATIVE_INFINITY;
            Double end0 = Double.POSITIVE_INFINITY;
            for(int i = 0; i < events.length; ++i) {
                start0 = Math.max(start0, events[i].getStart());
                end0 = Math.min(end0, events[i].getEnd());
            }

            times = getSampleTimes(conn, events[0].getID(), start0, end0, filter.getTimeCoordinateString(), filter.getTimeCache());
        }

        final int n = times.length;
        final double start = times[0];
        final double end = times[n-1];

        Object[][] rows = new Object[n][];
        for(int i = 0; i < n; ++i) {
            rows[i] = new Object[events.length+1];
            rows[i][0] = times[i];
        }


        // The considerations for this algorithm are:
        // 1) values for each timeseries are interpolated in one pass (linear time)
        // 2) the samples for only one timeseries are resident in memory at a time, and held only in the ResulSet

        // fill in each column of rows, which corresponds to a timeseries.
        for(int iID = 0; iID < events.length; ++iID) {
            Event ev = events[iID];
            final Integer dim = H2STimeseries.getDim(conn, ev.getID());
            if(dim==null)
                continue; // not a timeseries.  This column will be null.
            Timeseries q = new Timeseries(ev.getID(), ev.getStart(), ev.getEnd(), ev.getTagset(), dim); // use this to hold the most recent couple samples to make interpolations.

            // get all the values >= start and <= end, plus 2 before and after for interpolation.
            // The reason we must get 2 (instead of just 1) before and after is for extrapolating a value
            // before the first observation (or after the last)
            ResultSet samples = getSamples(conn, ev.getID(), start, end, 2, 2, filter.getTimeCoordinateString(), filter.getTimeCache());
            if(!samples.next()) {
                samples.close();
                continue; // all values for this timeseries will be null
            }
            q.add(new Sample(samples));

            // initial two values can contribute to an extrapolated value before the first observation.
            if(samples.next())
                q.add(new Sample(samples));

            // now set the result for each timestep.
            for(int t = 0; t < n; ++t) {
                while(q.getSample(q.getNumSamples()-1).getTime() < times[t] && samples.next()) {
                    q.add(new Sample(samples));
                    if(q.getNumSamples() > 1000) // arbitrary number; don't keep too many unnecessarily but don't resize too often.
                        q.keepNewest(2); // at most two samples needed to interpolate a value.
                }
                q.keepNewest(2); // having only two in timeseries prevents need for binary search inside valueLinear
                rows[t][1+iID] = q.valueLinear(times[t]); // time conversion was done already by Timeseries.getCopies()
            }
            samples.close();
        }

        // the resultSet is the transpose of the columns
        for(int i = 0; i < n; ++i)
            result.addRow(rows[i]);

        return result;
    }

    public static Double[] getSampleTimes(Connection conn, long id, Double start, Double end, String timeCoordinate, H2STime.Cache timeCache) throws SQLException {
        ArrayList<Double> times = new ArrayList<Double>();
        ResultSet rs = getSamples(conn, id, start, end, 0, 0, timeCoordinate, timeCache);
        while(rs.next())
            times.add(rs.getDouble(1));
        rs.close();
        return times.toArray(new Double[0]);
    }

    /**
     * Retrieve samples for the specified timeseries, including those at or after
     * after 'start' plus the last 'numBefore' samples before 'start',
     * and likewise for end/numAfter.
     * <p>
     * The returned columns are:<br>
     * TIME, SAMPLE<br>
     * where each row of DATA is an array.
     * and can be parsed by Timeseries.SampleCopy(ResultSet rs)
     * <p>
     * dataDims is the number of dimensions of rows to retrieve.  If 0, only the times are returned.
     * This number may not be larger than the dimensionality of the timeseries.
     * <p>
     * If timeCoordinate is non-null, then start/end are specified in this time coordinate
     * and the times of the returned samples are converted to it.  If non-null, timeCache
     * is used to get the time conversion parameters.
     * <p>
     * start and/or end may be null.  In this case all the samples starting from
     * the first (or up the last) are returned, and numBefore (or numAfter) is ignored.
     *
     */
    public static ResultSet getSamples(Connection conn, long id, Double start, Double end, int numBefore, int numAfter,
            String timeCoordinate_, H2STime.Cache timeCache) throws SQLException {

        //// to efficiently limit results to the specified start/end times, must convert the start/end times.
        // the default constructor for TimeCoordinateParamaters creates a mapping that has no effect.
        H2STime.TimeCoordinateParameters convertTime = new H2STime.TimeCoordinateParameters();
        if(timeCoordinate_ != null) { // convert from the time coordinate for this particular event to the output time coordinate.
            String tags = H2SEvent.getTagsByID(conn, id);
            if(timeCache != null)
                convertTime = timeCache.getConversionParameters(conn, new Tagset(tags), new Tagset(timeCoordinate_));
            else
                convertTime = H2STime.getConversionParameters(conn, tags, timeCoordinate_);
        }

        final String tableAndSchema=schemaName+"."+getTableName(conn, id);

        PreparedStatementCache.Query q = PreparedStatementCache.startQuery(conn);

        // first, create the names of columns to retrieve.
        if(timeCoordinate_ == null)
            q.add("select TIME"); // get time as-is, and the inner queries will get the rows columns or not depending on includeData.
        else
            q.addAlt("select TIME*",convertTime._m, "+", convertTime._b, " TIME");

        final Integer dataDims = H2STimeseries.getDim(conn, id);
        if(dataDims == null)
            throw new SQLException("H2STimeseries error: getSamples called with (non-Timeseries) Event ID " + id);
        q.add(",(C1");
        for(int i=1; i < dataDims; ++i)
            q.add(",C"+(i+1));
        q.add(") SAMPLE");

        q.addAlt(" from "+tableAndSchema+" where EVENT_ID=", id);

        if(numBefore > 0 && start != null)
            start = convertTime.map(timeOfNthBefore(conn, tableAndSchema, id, convertTime.unmap(start), numBefore));
        if(numAfter > 0 && end != null)
            end = convertTime.map(timeOfNthAfter(conn, tableAndSchema, id, convertTime.unmap(end), numAfter));

        if(start != null && end != null)
            q.addAlt(" and TIME BETWEEN ", convertTime.unmap(start), " AND ", convertTime.unmap(end));
        else if(start != null)
            q.addAlt(" and TIME >= ", convertTime.unmap(start));
        else if(end != null)
            q.addAlt(" and TIME <= ", convertTime.unmap(end));
        q.add(" order by TIME");

        return q.getPreparedStatement().executeQuery();

    }

    public static double timeOfNthBefore(Connection conn, String schemaAndTableName, long id, Double time, int n) throws SQLException {
        // This query is more complicated than timeOfNthAfter because H2 is not able
        // to optimize the query of finding the last value before a specified time
        // using its index.
        // The previous solution was a computed MINUSTIME column that was the negative of time
        // which was also indexed.  But this had a very large storage overhead.
        PreparedStatementCache.Query q;

        // first try a guess based on sample rate after the query time.
        final double after = timeOfNthAfter(conn, schemaAndTableName, id, time, n);
        double dt = after-time;
        if(dt <= 0) // there were no samples after the query time
            dt = 0.1; // arbitrary guess
        for(int iGuess=0; iGuess < 5; ++iGuess, dt *= 10) { // *=10 implements exponential backoff - get more history
            q = PreparedStatementCache.startQuery(conn);
            q.addAlt("select TIME from "+schemaAndTableName+" where EVENT_ID=", id, " and time >=", (time-2*dt), " and time < ", time, " order by TIME desc limit ",n);
            // System.err.println(q);
            ResultSet rs = q.getPreparedStatement().executeQuery();
            int gotBefore=0;
            while(rs.next()) {
                if(++gotBefore == n) {
                    double result = rs.getDouble(1);
                    rs.close();
                    return result;
                }
            }
            rs.close();
        }

        // here is a non-optimzed query that will find the correct result.
        q = PreparedStatementCache.startQuery(conn);
        q.addAlt("select min(TIME) from (select * from "+schemaAndTableName+" where EVENT_ID=", id, " and time < ", time, " order by time desc limit ", n, ");");
        ResultSet rs = q.getPreparedStatement().executeQuery();
        if(rs.next())
            time = rs.getDouble(1); // if no result, just return the time passed in.
        rs.close();
        return time;
    }

    public static double timeOfNthAfter(Connection conn, String schemaAndTableName, long timeseriesID, double time, int n) throws SQLException {
        PreparedStatementCache.Query q = PreparedStatementCache.startQuery(conn);
        q.addAlt("select max(TIME) TIME from (select TIME from "+schemaAndTableName+" where EVENT_ID=",timeseriesID," and time > ",time," limit ",n,");");
//        System.err.println(q);
        ResultSet rs = q.getPreparedStatement().executeQuery();
        if(rs.next() && rs.getDouble(1) > time)
            time = rs.getDouble(1);
        return time;
    }

   /**
    * Retrieve the value of the specified timeseries using linear interpolation.
    * If there is a sample before and after the time, those two samples will be used.
    * Otherwise, the value will be extrapolated from the last two values before the specified time (or first two after the specified time, as the case may be).
    * If there is only one sample, its value is assumed constant for all time.
    * If there are no samples, an exception is thrown.
    * If a timeCoordinate is passed, it is a tagset specifying a time coordinate for the 'time' parameter, e.g. 'timeCoordinate=sessionSeconds,session=2010_04_21__17_24_29'
    * timeCoordinate may be null, in which case the 'time' parameter is interpreted to be in whatever time coordinate the timeseries is stored.
    **/
    public static Float[] valueLinear(Connection conn,
        long id,
        Double time,
        String timeCoordinate)
        throws SQLException
    {
        int dim = getDim(conn, id);

        if(timeCoordinate != null)
            time = H2SEvent.convertTime(conn, time, timeCoordinate, id);

        ResultSet rs = getSamples(conn, id, time, time, 2, 2, null, null);

        Timeseries tc = new Timeseries(id, -Double.MAX_VALUE, Double.MAX_VALUE, null, dim);
        while(rs.next())
            tc.add(new Sample(rs));
        if(tc.getNumSamples()==0)
            throw new SQLException("Error retrieving value for timeseries " + id + "; it's empty");

        return tc.valueLinear(time);
    }

    /*
     * return true iff there is a timeseries with the specified ID
     */
    public static boolean isTimeSeries(Connection conn, long ID) throws SQLException
    {
        String[] tableNames = H2SEvent.attachedDataTables(conn, ID, schemaName);
        return tableNames.length > 0;
    }

    public static void interpolate(double t1, Float[] x1, double t2, Float[] x2, double resultTime, Float[] result) {
        if(x2.length != x1.length)
            throw new IllegalArgumentException("H2STimeseries.interpolate: the two source arrays are of different dimension");
        if(result.length != x1.length)
            throw new IllegalArgumentException("H2STimeseries.interpolate: the output dimension doesn't match the input");
        if(resultTime == t1) {
            System.arraycopy(x1, 0, result, 0, x1.length);
            return;
        }
        if(resultTime == t2) {
            System.arraycopy(x2, 0, result, 0, x2.length);
            return;
        }
        int dim = x1.length;
        final double f = (resultTime - t1) / (t2 - t1);
        for (int i = 0; i < dim; ++i)
            result[i] = new Float(x1[i] + (x2[i] - x1[i]) * f);
    }

    public static Float[] interpolate(double t1, Float[] x1, double t2, Float[] x2, double resultTime) {
        if(resultTime == t1)
            return x1;
        if(resultTime == t2)
            return x2;
        Float[] result = new Float[x1.length];
        interpolate(t1, x1, t2, x2, resultTime, result);
        return result;
    }

    /**
     * Finds Timeseries events only.
     * See H2SEvent.find() for full documentation.
     * <p>
     * This is not called from elsewhere in the RBB code but an alias is
     * created for it in create_rbb.sql (so don't remove it)
     */
    public static ResultSet find(Connection conn,
        String filterTags,
        Double start,
        Double end,
        Object[] IDs,
        String timeCoordinate) throws SQLException {
        return H2SEvent.find(conn, filterTags, start, end, IDs, timeCoordinate, schemaName);
    }

    /**
     * Find the timeseries whose values are nearest the specified point at the specified time, sorted by distance (low to high)
     * Example sql: call rbb_find_nearest_timeseries(('test','draw'), (972, 367), 6.416, null, 50, 99);
     *
     * @param conn
     * @param filterTags: Only timeseries with at least these tags will be found.  Not null.
     * @param pt: the target point.  Not null.
     * @param time: the target time.  not null.
     * @param maxDist: If not null, only results within this distance will be found.
     * @param maxResults: If not null, will return at most this many results.
     *
     * @return The returned ResultSet contains the same columns as from H2SEvent.find, plus:
     * X: the value of the timeseries at the specified time.
     * DIST: the distance from the query point
     *
     * example:
     * ID START_TIME END_TIME  	TAGS  	                                  DATA_SCHEMA    DATA_TABLE  X  	                   DIST
     * 3  0.729	     9.184	    (color, red, test, draw, type, drawing)	  RBB_TIMESERIES S3	         (936.25397, 377.96826) 37.39093
     * 5  5.368	     9.641	    (color, green, test, draw, type, drawing) RBB_TIMESERIES S5	         (986.17163, 406.17163) 41.656353
     */
    public static ResultSet findNearest(Connection conn,
            String filterTags,
            Object[] pt,
            double time,
            String timeCoordinate,
            Double maxDist,
            Integer maxResults) throws SQLException {

        // find timeseries that exist at the time and match the filter tags.
        String q = "rbb_find_timeseries(?, "+time+", "+time+", null, ?)"; // ? will be set to the filterTags.

        // retrieve the value of each as of the specified time.
        q = "select *, rbb_timeseries_value(ID, "+time+", ?, null) as X from "+q;

        // calculate the distance from the query point to each timeseries.
        q = "select *, rbb_distance(X, ?) as DIST from ("+q+") order by DIST"; // ? will be the query point.

        // limit number of results if desired.
        if(maxResults != null)
            q += " limit " + maxResults;

        // limit the max distance if desired.
        if(maxDist != null)
            q = "select * from (" + q + ") where DIST <= "+maxDist+";";

        if(filterTags != null)
            filterTags = new Tagset(filterTags).toString(); // this converts from string representation to array
        
        if(timeCoordinate != null)
            timeCoordinate = new Tagset(timeCoordinate).toString(); // this converts from string representation to array

        PreparedStatement prep = conn.prepareStatement(q);
        prep.setObject(1, pt);
        prep.setObject(2, timeCoordinate);
        prep.setObject(3, filterTags);
        prep.setObject(4, timeCoordinate);

        // now q is a query like:
        // select * from (select *, rbb_distance(X, (972.0,367.0)) as DIST from (select *, rbb_timeseries_value(ID, 6.416, ?, null) as X from rbb_find_timeseries(('test','draw'), 6.416, 6.416, ?)) order by DIST limit 99) where DIST <= 50;
        // System.err.println(q);

        return prep.executeQuery();
    }



}
