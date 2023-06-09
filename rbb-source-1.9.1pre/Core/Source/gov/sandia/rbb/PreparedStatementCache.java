/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

/**
 * This class is useful when you have some code that builds up complex queries,
 * (e.g. including calling other methods to construct portions of it),
 * which may or may not be the same each time.
 *<p>
 * To use this class call:
 * PreparedStatementCache.Query q = PreparedStatementCache.startQuery(connection);
 * q.add("SELECT * from DATA where x=");
 * q.addParam(6);
 * ResultSet rs = q.getPreparedStatement().executeQuery(); // or whatever you want to do with the PreparedStatement.
 *<p>
 * Re-build the query as above for every query.  A preparedStatement will be used
 * if the query (other than the parameters) turns out the same as a previous one.
 *<p>
 * If you want to avoid using the preparedStatement, you can
 * also create the Query (calling add and addParam) but then call
 * conn.createStatement().executeQuery(q.toString());
 * but note this won't work if any of the parameters need to be quoted / escaped.
 *<p>
 * THREADING Note:  If multiple threads are operating on a  Connection, bad things 
 * will happen if they execute the same PreparedStatement since they will be sharing
 * the same ResultSet instance.  When using this class, if there are other threads
 * accessing the Connection, you don't normally know whether they are executing the
 * same queries (and thus using the same PreparedStatement instance) as you.  So
 * the convention for multi-threaded code in RBB is you must synchronize on the
 * Connection instance from when you call Query.getPreparedStatement() until you
 * are done with the result set.
 *<p>
 * NOTE: in H2, executing a preparedStatemnt closes the ResultSet from the previous
 * execution.  That means you can't use PreparedStatement for nested executions of the
 * same query.
 *<p>
 * NOTE: this class does not ever actually call close() on a PreparedStatement because
 * it has no way to know if it is in a subquery and the outer query is using a given
 * PreparedStatement.  Instead it just randomly evicts PreparedStatements from the cache.
 * It also randomly evicts Connections from the cache.  This is important because
 * connection.isClosed() never returns true in many cases; I think H2 is creating
 * temporary Connection instances for subqueries etc, which are attached to a lower-level
 * object that is not closed so long as the database itself is open.  So these
 * temporary Connection instances pile up and can't be garbage collected unless they
 * are un-cached.
 *<p>
 * Hypothetical bug: h2 seems to re-use Connection instances from a pool.
 * (Because in the test code for this class, this cache doesn't always create a
 * new instance even for a newly created RBB).
 * If a preparedStatent is created from a connection which is closed, then re-opened
 * on another database, what happens if the preparedStatement is then executed?
 *
 * @author rgabbot
 */
public class PreparedStatementCache {

    public static synchronized Query startQuery(Connection conn) throws SQLException {
        if(conn==null)
            throw new SQLException("PreparedStatementCache.startQuery: null connection specified.");
        PreparedStatementCache psCache = perConnection.get(conn);
        if(psCache==null) {
            trimConnections();
            psCache = new PreparedStatementCache(conn);
            perConnection.put(conn, psCache);
        }

        return psCache.newQuery();
    }

    private static void trimConnections() throws SQLException {
        final int maxSize = 1000; // no particular effort went into chosing the value of 1000
        
        if(perConnection.size() < maxSize)
            return; // is small enough

        // remove some of the Prepared Statements for each Connection,
        // and remove the entire connection if it is then empty, or if it is closed.
        Random rand = new Random(); // each prepared statement will be ejected from cache with 10% probability.
        Iterator<Map.Entry<Connection,PreparedStatementCache>> connIter = perConnection.entrySet().iterator();
        while(connIter.hasNext()) {
            PreparedStatementCache psc = connIter.next().getValue();
            psc.trimPreparedStatements(rand);
            if(psc.stmts.isEmpty() || rand.nextInt(10)==0) {
                connIter.remove();
            }
        }
    }

    public class Query {

        private ArrayList<Object> elem = new ArrayList<Object>();

        /**
         * The indexes of elements in elem that are parameters.
         */
        private ArrayList<Integer> paramIndexes = new ArrayList<Integer>();

        public void add(String... a) {
            for(String s : a)
                elem.add(s);
        }

        public void addParam(Object o) {
            paramIndexes.add(elem.size());
            elem.add(o);
        }

        /*
         * Alternate between add and addParam - every other argument is a parameter
         */
        public void addAlt(Object... a) {
            for(int i = 0; i < a.length; ++i)
                if(i%2==0)
                    add(a[i].toString());
                else
                    addParam(a[i]);
        }

        /*
         * Add a comma-separated array of parameters, starting and ending with parens.
         */
        public void addParamArray(Object... a) {
            add("(");
            for(int i=0; i < a.length; ++i) {
                if(i>0)
                    add(",");
                addParam(a[i]);
            }
            add(")");
        }

        /**
         * Get a preparedStatement for the query that has been built up with add(),
         * recycling the PreparedStatement if possible,
         * loaded up with the parameter values specified while building the query.
         * <p>
         * Do not call close() on the returned PreparedStatement(), since the whole
         * point of this class is to return the same instance later if possible.
         * However, it is fine to call close() on the ResultSet from the query.
         */
        public PreparedStatement getPreparedStatement() throws SQLException {
            final String query = toString(false);
            PreparedStatement ps;

            // check whether this query was already in stmts (the cache), and if not,
            // put it there... This must be synchronized on stmts since we are
            // checking its state then altering its state based on the result.
            synchronized(stmts) {

                ps = stmts.get(query);

    //            if(ps != null)
    //                System.err.println("Re-using preparedStatement "+query);

                if(ps==null) { // was not found in cache
    //                System.err.println("New preparedStatement "+query);
                    if(stmts.size() >= 1000) // no particular effort went into chosing the value of 1000
                        trimPreparedStatements(new Random(10));
                    ps = conn.prepareStatement(query);
                    stmts.put(query, ps);
                    ++numPreparedStatements;
                }
                else {// re-use an old one
                    if(ps.isClosed())
                        throw new SQLException("PreparedStatementCache.getPreparedStatement error: found a cached prepared statement for " + toString() + " but it was closed");
                }

            }

            for(int i = 0; i < paramIndexes.size(); ++i) {
                final Object parameterValue = elem.get(paramIndexes.get(i));
                ps.setObject(i+1, parameterValue);
            }

            return ps;
        }

        @Override
        public String toString() {
            return toString(true);
        }

        private String toString(boolean showParameterValues) {
            StringBuilder sb = new StringBuilder();
            int p = 0; // index of next parameter
            for(int i = 0; i < elem.size(); ++i) {
                if(showParameterValues==false && p < paramIndexes.size() && paramIndexes.get(p)==i) {
                    ++p;
                    sb.append("?"); // parameter slot in stored procedure
                }
                else
                    sb.append(elem.get(i));
            }
            return sb.toString();
        }

        /*
         * equals and hashCode are needed because the implementation creates a hash of Queries.
         */
        @Override
        public boolean equals(Object q)
        {
            if(q==null)
                return false;
            if(!(q instanceof Query))
                return false;

            return toString(false).equals(((Query)q).toString(false));
        }

        /*
         * equals and hashCode are needed because the implementation creates a hash of Queries.
         */
        @Override
        public int hashCode()
        {
            return toString(false).hashCode();
        }
    }

    private void trimPreparedStatements(Random rand) throws SQLException {
        if(conn.isClosed()) { // if the connection is closed, all the PreparedStatements on it are invalidated, and trying to call close() on them just raises an exception.
            stmts.clear();
//            System.err.println("connection was closed - ");
            return;
        }
    //    System.err.println("initial size was: "+stmts.size());
        Iterator<Map.Entry<String,PreparedStatement>> iter = stmts.entrySet().iterator();
        while(iter.hasNext()) {
            PreparedStatement ps = iter.next().getValue();
            if(rand != null && rand.nextInt(10) != 0)
                continue; // we are randomly picking losers and this one got lucky.
            iter.remove();
        }
//        System.err.println("size after trimming now: "+stmts.size());
    }

    private static Map<Connection, PreparedStatementCache> perConnection = new HashMap<Connection, PreparedStatementCache>();

    private Connection conn;
    private Map<String, PreparedStatement> stmts;

    private PreparedStatementCache(Connection conn) {
        this.conn = conn;
        stmts = new HashMap<String, PreparedStatement>();
    }

    /*
     * This might appear pointless, but is necessary because Query is (by design)
     * a non-static inner class of PreparedStatementCache.
     */
    private Query newQuery() {
        return new Query();
    }

    /**
     * These are for testing and may change arbitrarily at any time from inside or outside this class.
     */
    static int numConnections;
    static int numPreparedStatements;

    /*
     * This is a utility class for building delimited lists.
     * It is constructed with a list of strings.  The first call to get()
     * returns the first string, and so on until the last string which is
     * returned thereafter.
     */
    public static class Delim {
        String[] delims;
        int pos;
        public Delim(String... delims) {
            this.delims=delims;
            pos=0;
        }
        /*
         * Constructor for comma-separated values: get "" the first time, "," thereafter.
         */
        public static Delim CSV() {
            return new Delim("", ",");
        }
        
        public String get() {
            if(pos >= delims.length)
                return delims[delims.length-1];
            else
                return delims[pos++];
        }
    }
}
