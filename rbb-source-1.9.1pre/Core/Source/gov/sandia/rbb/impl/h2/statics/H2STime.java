/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.impl.h2.statics;

import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.PreparedStatementCache;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Contains public static methods for manipulating times, such as converting from one 
 * timeframe to another.
 *
 * NOTE:
 * This class can either be used entirely statically, or a Cache can be instantiated so calls are cached.
 * The static versions are incredibly slow, so the H2STime.Cache should be used
 * whenever more than a few time conversions will be done.
 *
 *
 * @author rgabbot
 */
public class H2STime {

    /*
     * This method implements the SQL call RBB_TIME_COORDINATE_FROM_TAGSET which is useful inside some larger queries.
     */
    public static String coordinateFromTagset(String tags)
        throws java.sql.SQLException
    {
        return new Tagset(tags).getValue("timeCoordinate");
    }

    /**
     * Return the set of tags that condition a given type of time coordinate, e.g. "secondsUTC"
     * If there is no such time coordinate, returns null.
     * All instances of a timeCoordinate must use the same set of tag names only the first is consulted).
     * @param conn
     * @param timeCoordinate
     * @return
     * @throws java.sql.SQLException
     */
    private static Set<String> getTagsRequiredForCoordinate(
        Connection conn,
        String timeCoordinate)
        throws java.sql.SQLException
    {
        String q = "select RBB_ID_TO_TAGSET(TAGSET_ID) as TAGS from rbb_time_coordinates where time_coordinate_string_id = rbb_string_to_id(?) limit 1";
        java.sql.PreparedStatement ps = conn.prepareStatement(q);
        ps.setString(1, timeCoordinate);
        ResultSet rs = ps.executeQuery();
        if (!rs.next())
            return null;

        gov.sandia.rbb.Tagset tags = new Tagset(rs.getString("TAGS"));
        return tags.getNames();
    }

    /**
     * Define a time coordinate by explicitly specifying all parameters.
     * This isn't much different than INSERTing a row into RBB_TIME_COORDINATES,
     * except it checks that if another timeCoordinate by the same name has already
     * been defined, it is conditioned on the same set of tags.
     * @param conn
     * @param tagset must specify "timeCoordinate=<name>", and any additional
     * tags on which the timeCoordinate is conditioned (including their values), e.g
     * "timeCoordinate=secondsUTC" or
     * "timeCoordinate=sessionSeconds,session=1"
     * @param slope scaling factor (m in y=mx+b) to convert from UTC to the new coordinate, e.g.
     * if UTC is seconds and the new timeCoordinate is in milliseconds, would be 1000.0
     * @param intercept offset (b in y=mx+b) to convert from UTC to the new coordinate, e.g.
     * if UTC and the new coordinate are both in seconds, but the new coordinate starts 10 seconds after UTC,
     * would be -10
     * @throws java.sql.SQLException
     */
    public static void defineCoordinate(Connection conn,
        String   tagset_,
        double slope,
        double intercept)
        throws java.sql.SQLException
    {
        // ensure that if another timeCoordinate by the same name has already been defined, it is conditioned on the same tags (if any)
        Tagset tagset = new Tagset(tagset_);
        String timeCoordinateType = tagset.getValue("timeCoordinate");
        if (timeCoordinateType == null)
        {
            throw new java.sql.SQLException("Error - tried to define a time coordinate with the tagset "
                + tagset + " which does not have a timeCoordinate tag");
        }
        Set<String> tagsRequired = getTagsRequiredForCoordinate(conn,
            timeCoordinateType);
        if (tagsRequired != null && !tagset.getNames().equals(tagsRequired))
        {
            String err = "Error - attempt to define time coordinate "
                + timeCoordinateType + " with tagset " + tagset
                + " but it was previously defined conditioned on tags:";
            for (Iterator<String> i = tagsRequired.iterator(); i.hasNext();)
                err += " '" + i.next() + "'";
            throw new java.sql.SQLException(err);
        }
        // ok, it passed the test.

        // create the time coordinate
        final long tagsetID = H2STagset.toID(conn, tagset_);
        conn.createStatement().execute("insert into rbb_time_coordinates values("
            + tagsetID + ", " + slope + ", " + intercept + ",0);");
    }

    
    public static class TimeCoordinateParameters
    {

        public TimeCoordinateParameters()
        {
            _m = 1;
            _b = 0;
        }

        public TimeCoordinateParameters(double m, double b)
        {
            _m = m;
            _b = b;
        }

        public TimeCoordinateParameters(TimeCoordinateParameters copyOf) {
            this(copyOf._m, copyOf._b);
        }

        public String toString()
        {
            return "*" + _m + "+" + _b;
        }

        public double map(double x)
        {
            return _m * x + _b;
        }

        public String mapString(String x)
        {
            if(_m==1 && _b==0)
                return x;
            return "("+_m+"*"+x+"+"+_b+")";
        }

        public double unmap(double y)
        {
            // y = m * x + b
            // y - b = m * x
            // (y-b)/m = x
            return (y-_b)/_m;
        }

        public String unmapString(String y)
        {
            if(_m==1 && _b==0)
                return y;
            // the extra parens around _b are necessary in case _b < 0
            return "(("+y+"-("+_b+"))/"+_m+")";
        }

        public double _m, _b;

    }

    /** retrieve the time coordinate parameters (slope, intercept) matching the tagSet.
    A 'default' set of tags may be provided in the optional parameter defaultTagString.
     */
    static TimeCoordinateParameters getCoordinateParameters(
        Connection conn,
        String tags_,
        String defaultTags_)
        throws java.sql.SQLException
    {
        Tagset tags = new Tagset(tags_);
        Tagset defaultTags = (defaultTags_ == null ? null : new Tagset(defaultTags_));
        String timeCoordinate = tags.getValue("timeCoordinate");
        if (timeCoordinate == null)
        {
            throw new java.sql.SQLException("H2Statics.getCoordinateParameters error: no timeCoordinate tag in tagset "
                + tags.toString());
        }
        Set<String> requiredTags = getTagsRequiredForCoordinate(conn,
            timeCoordinate);
        if (requiredTags == null)
        {
            throw new java.sql.SQLException("getTimeCoordinateParameters("
                + timeCoordinate
                + ") failed because this time coordinate doesn't exist");
        }
        Tagset lookupTags = new Tagset();
        for (Iterator<String> i = requiredTags.iterator(); i.hasNext();)
        {
            String name = i.next();
            String value = tags.getValue(name);
            if (value == null && defaultTags != null)
            {
                value = defaultTags.getValue(name);
            }
            lookupTags.add(name, value);
        }
        // look up mapping parameters
        PreparedStatementCache.Query q = PreparedStatementCache.startQuery(conn);
        q.add("select * from rbb_time_coordinates where tagset_id in (");
        H2STagset.hasTagsQuery(conn, lookupTags.toString(), q);
        q.add(");");
        ResultSet rs = q.getPreparedStatement().executeQuery();

        if (!rs.next())
        {
            throw new java.sql.SQLException("no time coordinates named "
                + timeCoordinate + " with join tags " + lookupTags.toString());
        }
        final TimeCoordinateParameters tcp = new TimeCoordinateParameters(rs.getDouble(
            2), rs.getDouble(3));
        if (rs.next())
        {
            throw new java.sql.SQLException("Error: multiple time coordinates named "
                + timeCoordinate + " have join tags " + lookupTags.toString());
        }
        return tcp;
    }

    public static TimeCoordinateParameters getConversionParameters(
        Connection conn,
        String fromTags,
        String toTags)
        throws java.sql.SQLException
    {
        TimeCoordinateParameters from = getCoordinateParameters(conn,
            fromTags, toTags);

        // The 'from" tagset provides defaults for the 'to' tagset.
        // Typically, the 'to' tagset includes at least the 'timeCoordinate' tag, and other tags on which that timeCoordinate is conditioned may come from the 'from' tags.
        TimeCoordinateParameters to = getCoordinateParameters(conn, toTags, fromTags);

        // System.err.println("from: " + from.toString());
        // System.err.println("to  : " + to.toString());
        return new TimeCoordinateParameters(to._m / from._m, -from._b * to._m / from._m + to._b);
    }

    /**
     * a. with a null timeCoordinate, no time conversion is performed.  (This means the START and END arguments to findSequence are meaningless if the database contains sequences stored in multiple timeCoordinates that match the filterTags).
     * b. the timeCoordinate parameter is passed as a taglist that must include a timeCoordinate (e.g. "timeCoordinate=conditionSeconds") but may include other tags specifying time conversion parameters, which take precedence over the timeCoordinate parameters in the sequence taglist.
     * c. if given a timeCoordinate parameter, then the START and END parameters are interpreted as being in that time coordinate.
     * d. conversion between time coordinates implies converting both to UTC, so sufficient context tags must be present for all timeCoordinates in sequences matching the filterTags
     * e. The START and END time in the sequence infos returned by findSequences are in the requested output time coordinate.
     * f. The tagset returned includes the native timeCoordinate tag (not the output timeCoordinate tag)
     *
     * @param conn
     * @param t
     * @param fromTags
     * @param toTags
     * @return
     * @throws java.sql.SQLException
     */
    public static double convert(Connection conn,
        double t,
        String fromTags,
        String toTags)
        throws java.sql.SQLException
    {
        TimeCoordinateParameters p = getConversionParameters(conn, fromTags, toTags);
        return p._m * t + p._b;
    }

    public static double toUTC(Connection conn,
        double t,
        String tags)
        throws java.sql.SQLException
    {
        TimeCoordinateParameters p = getCoordinateParameters(conn, tags, null);
        return (t - p._b) / p._m;
    }

    public static double fromUTC(Connection conn,
        double t,
        String tags)
        throws java.sql.SQLException
    {
        TimeCoordinateParameters p = getCoordinateParameters(conn, tags,
            null);
        return p._m * t + p._b;
    }

    public static int deleteCoordinates(Connection conn, String filterTags)
        throws java.sql.SQLException
    {
        PreparedStatementCache.Query q = PreparedStatementCache.startQuery(conn);
        q.add("delete from RBB_TIME_COORDINATES where TAGSET_ID in (");
        H2STagset.hasTagsQuery(conn, filterTags, q);
        q.add(")");
        return q.getPreparedStatement().executeUpdate();
    }

    public static class Cache {
        private Map<String, Set<String>> tagsRequiredForCoordinate;
        private Map<Tagset, TimeCoordinateParameters> timeCoordinateParamaters;

        /**
         * this retrieves the parameters to convert from the specified timeCoordinate
         * to UTC... NOT to convert from tags to defaultTags.  Make sure you didn't
         * want getConversionParameters instead!
         */
        public TimeCoordinateParameters getCoordinateParameters(
            Connection db,
            Tagset tags,
            Tagset defaultTags)
            throws SQLException
        {
            String timeCoordinate = tags.getValue("timeCoordinate");
            if (timeCoordinate == null)
                throw new java.sql.SQLException("TimeCoordinates error: no timeCoordinate tag in tagset " + tags.toString());

            Set<String> requiredTags = getTagsRequiredForCoordinate(db, timeCoordinate);

            if (requiredTags == null)
                throw new java.sql.SQLException("getTimeCoordinateParameters("
                    + timeCoordinate
                    + ") failed because this time coordinate doesn't exist");

            Tagset lookupTags = new Tagset();
            for(String tagName : requiredTags) {
                String tagValue = tags.getValue(tagName);
                if (tagValue == null && defaultTags != null)
                    tagValue = defaultTags.getValue(tagName);
                if(tagValue == null)
                    throw new SQLException("Error: " + tagName + " is required for time coordinate "+timeCoordinate);
                lookupTags.add(tagName, tagValue);
            }

            // look up mapping parameters
            if(timeCoordinateParamaters==null)
                timeCoordinateParamaters = new HashMap<Tagset, TimeCoordinateParameters>();
            TimeCoordinateParameters result = timeCoordinateParamaters.get(lookupTags);

            // cache miss could be for one of the following reasons:
            // 1) This cache hasn't mapped this time coordinate before
            // 2) This time coordinate was mapped, but before a new time coordinate using these particular time coordinate parameters was defined.
            // 3) Invalid time coordinate parameters were specified.
            if(result == null) { // in any case, simply re-read all time coordinates of this type.
                String q = "select rbb_id_to_tagset(tagset_id) as TAGS, SLOPE, INTERCEPT from rbb_time_coordinates where time_coordinate_string_id = rbb_string_find_id(?);";
                PreparedStatement stmt = db.prepareStatement(q);
                stmt.setString(1, timeCoordinate);
                ResultSet rs = stmt.executeQuery();
                while(rs.next()) {
                    Tagset tcTags = new Tagset(rs.getString("TAGS"));
                    timeCoordinateParamaters.put(tcTags, new TimeCoordinateParameters(rs.getDouble("SLOPE"), rs.getDouble("INTERCEPT")));
                }

                // now try the lookup again.
                result = timeCoordinateParamaters.get(lookupTags);
                if(result == null)
                    throw new SQLException("Error: time coordinate " + timeCoordinate + " is defined, but not with parameters: "+lookupTags);
            }

            return result;
        }

        /**
         *
         * Caches calls to H2STime.tagsRequiredForCoordinate.
         *
         * We could almost just make a DETERMINISTIC SQL alias to let H2 cache this,
         * except it could be called on an undefined timeCoordinate, which is then
         * defined, thus its result changes.
         *
         */
        private Set<String> getTagsRequiredForCoordinate(Connection db, String timeCoordinate) throws SQLException
        {
            if(tagsRequiredForCoordinate == null)
                tagsRequiredForCoordinate = new HashMap<String, Set<String>>();
            Set<String> result = tagsRequiredForCoordinate.get(timeCoordinate);
            if(result == null) {
                result = H2STime.getTagsRequiredForCoordinate(db, timeCoordinate);
                if(result != null)
                    tagsRequiredForCoordinate.put(timeCoordinate, result);
            }
            return result;
        }


        public double convert(Connection conn,
            double t,
            Tagset fromTags,
            Tagset toTags) throws SQLException {
                TimeCoordinateParameters p = getConversionParameters(conn, fromTags, toTags);
                return p._m * t + p._b;
            }

        public TimeCoordinateParameters getConversionParameters(
            Connection conn,
            Tagset fromTags,
            Tagset toTags)
            throws java.sql.SQLException
        {
            TimeCoordinateParameters from = getCoordinateParameters(conn,
                fromTags, toTags);

            // The 'from" tagset provides defaults for the 'to' tagset.
            // Typically, the 'to' tagset includes at least the 'timeCoordinate' tag, and other tags on which that timeCoordinate is conditioned may come from the 'from' tags.
            TimeCoordinateParameters to = getCoordinateParameters(conn,
                toTags, fromTags);

            // System.err.println("from: " + from.toString());
            // System.err.println("to  : " + to.toString());
            return new TimeCoordinateParameters(to._m / from._m, -from._b * to._m / from._m + to._b);
        }
    } // end of Cache class

}
