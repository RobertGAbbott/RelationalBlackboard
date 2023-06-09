/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.impl.h2.statics;

import gov.sandia.rbb.PreparedStatementCache;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 *
 *
 * H2SString contains static methods that are stored procedures in the H2 implementation of RBB
 * The create_rbb.sql script creates aliases for these so they can be called through SQL.
 * The public functions here only accept / return datatypes for which H2 has a SQL mapping.
 *
 * @author rgabbot
 */
public class H2SString {
    
    private static int numCalls = 0;

    /**
     * Retrieve id for a string.
     * Returns null if the string has no ID.
     * Returns 0 for the null string.
     * @param conn
     * @param s
     * @return
     * @throws SQLException
     */
    public static Long find(Connection conn, String s) throws SQLException
    {
//        if(++numCalls % 10000 == 0)
//            System.err.println("Calls to H2SString.find: "+numCalls);
        if(s==null)
            return 0L;

        Long id = cacheGet(conn, s);
        if(id != null)
            return id;

        // using a preparedStatement here is one way to get the quoting right on the string, which is important.
        PreparedStatementCache.Query q = PreparedStatementCache.startQuery(conn);
        q.addAlt("select ID from RBB_STRINGS where STRING=", s);
        ResultSet rs = q.getPreparedStatement().executeQuery();
        if (!rs.next())
            return null;
        id = rs.getLong(1);
        cachePut(conn, s, id); // was in RBB but not cache
        rs.close();

        return id;
    }

    /**
     * Return a table
     * STRING, ID 
     * which maps each string in s that was already in RBB_STRINGS to
     * its corresponding ID.  No mapping is returned for the null string.
     * Note the results are not guaranteed to be in any particular order.
     */
    public static ResultSet findEach(Connection conn, Object[] s) throws SQLException {
        PreparedStatementCache.Query q = PreparedStatementCache.startQuery(conn);
        q.add("select STRING, ID from RBB_STRINGS where STRING in ");
        q.addParamArray(s);
        return q.getPreparedStatement().executeQuery();
    }

    /**
     * Like findEach but returns result as a hash from Strings to their IDs.
     */
    public static Map<String,Long> findSet(Connection conn, Object[] s) throws SQLException {
        ResultSet rs = H2SString.findEach(conn, s);
        Map<String, Long> result = new HashMap<String,Long>();
        while(rs.next())
            result.put(rs.getString("STRING"), rs.getLong("ID"));
        return result;
    }

    /**
     * Map each element of the array a to its corresponding id.
     * A may be multi-dimensional, and the result will preserve its structure.
     * Like find(), the result will be 0 for null elements of a,
     * and null for Strings with no mapping.
     */
    public static Object[] findArray(Connection conn, Object[] a) throws SQLException {
        Set<String> strings = new HashSet<String>();
        findStrings(a, strings);
        Map<String,Long> ids = findSet(conn, strings.toArray());
        Object[] result = new Object[a.length];
        return mapStrings(a, ids);
    }

    private static void findStrings(Object[] a, Set<String> result) {
        for(Object o : a)
            if(o == null)
                continue;
            else if(o instanceof Object[])
                findStrings((Object[])o, result);
            else
                result.add(o.toString());
    }

    private static Object[] mapStrings(Object[] a, Map<String,Long> ids) {
        Object[] result = new Object[a.length];
        for(int i = 0; i < result.length; ++i)
            if(a[i] == null)
                result[i] = new Long(0);
            else if(a[i] instanceof Object[])
                result[i] = mapStrings((Object[])a[i], ids);
            else
                result[i] = ids.get(a[i].toString());
        return result;
    }

    /**
     * Retrieve id for a string, or create a new id if necessary.
     * Returns 0L for null string.
     *
     * @param conn
     * @param s
     * @return
     * @throws SQLException
     */
    public static long toID(Connection conn, String s)
        throws SQLException
    {
        if(s==null)
            return 0L;

        Long id = find(conn, s);
        if (id != null)
            return id;

        return newID(conn, s);
    }

    /*
     * Creates a new ID for a string without checking if it already had an ID.
     * This used if findID just returned null.
     */
    static long newID(Connection conn, String s) throws SQLException {
        java.sql.PreparedStatement prep = conn.prepareStatement("insert into RBB_STRINGS (string) values (?);");
        prep.setString(1, s);
        prep.execute();
        return find(conn, s); // this is how we get the newly-generated ID.  TODO: is there a better way.
    }

    /**
     * Retrieve the string for a particular ID.
     * @throws SQLException
     */
    public static String fromID(Connection conn, long id) throws SQLException {
        if(id==0)
            return null;

        PreparedStatementCache.Query q = PreparedStatementCache.startQuery(conn);

        q.addAlt("select STRING from RBB_STRINGS where ID=", id);
        ResultSet rs = q.getPreparedStatement().executeQuery();
        if(!rs.next())
            throw new SQLException("H2SString error: invalid id "+id);
        final String s = rs.getString(1);
        rs.close();
        return s;
    }

    /**
     * Retrieve the string for an array of IDs.
     * @throws SQLException
     */
    public static String[] fromIDs(Connection conn, Object[] ids) throws SQLException {
        PreparedStatementCache.Query q = PreparedStatementCache.startQuery(conn);
        q.add("select STRING from table(id2 long=");
        q.addParamArray(ids);
        q.add(") left outer join rbb_strings on id=id2;");
        // System.err.println(q);
        PreparedStatement ps = q.getPreparedStatement();
        ResultSet rs = ps.executeQuery();
        String[] result = new String[ids.length];
        for(int i = 0; i < result.length; ++i) {
            if(!rs.next())
                throw new SQLException("H2SString.fromIDs error: expected "+result.length+" results but got only "+i);
            result[i] = rs.getString(1);
        }
        rs.close();
        return result;
    }


    private static final HashMap<String, Long> cache = new HashMap<String, Long>();
    private synchronized static void cachePut(Connection conn, String s, Long ID) throws SQLException {
        if(cache.size() >= 5000)
            decimate();
        cache.put(s+H2SRBB.getUUID(conn), ID);
    }
    private synchronized static Long cacheGet(Connection conn, String s) throws SQLException {
        return cache.get(s+H2SRBB.getUUID(conn));
    }
    /**
     * There is no way to remove a random element from a set or hashmap in java.util, so instead
     * remove a fraction of all entries periodically.
     */
    private synchronized static void decimate() {
        Random rand = new Random();
        Iterator<Map.Entry<String,Long>> iter = cache.entrySet().iterator();
        while(iter.hasNext()) {
            final String tc = iter.next().getKey();
            if(rand.nextInt(10)!=0)
                continue;
            // System.err.println("un-caching "+tc);
            iter.remove();
        }
    }

}
