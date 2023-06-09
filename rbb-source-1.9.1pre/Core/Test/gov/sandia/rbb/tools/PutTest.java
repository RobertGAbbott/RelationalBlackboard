/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.tools;


import java.sql.ResultSet;
import gov.sandia.rbb.Timeseries;
import gov.sandia.rbb.impl.h2.statics.H2STimeseries;
import gov.sandia.rbb.impl.h2.statics.H2SRBBTest;
import gov.sandia.rbb.RBB;
import java.io.*;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author rgabbot
 */
public class PutTest {
    @Test
    public void testPut()
        throws Exception
    {
        System.err.println("Entering "+ java.lang.Thread.currentThread().getStackTrace()[1].getMethodName());

        final String dbURL = "jdbc:h2:mem:testPut";
        RBB rbb = RBB.create(dbURL, null);


        ByteArrayOutputStream timeseries_bytes = new ByteArrayOutputStream();
        PrintStream timeseries = new PrintStream(timeseries_bytes, true);

        timeseries.println("variable=x,n=1,start=0.5"); // specify start time using a tag.
        timeseries.println("1,11,12");
        timeseries.println("2,21,22");
        timeseries.println("3"); // specifies the end time without adding an observation
        timeseries.println("");
        timeseries.println("variable=emptyTimeSeries"); // make a in with 0 observations.
        timeseries.println("3.5"); // just a time, no data.
        timeseries.println("variable=event,time=123"); // create a discrete event.
        timeseries.println("variable=x,n=2"); // specify a sequence with start and end times inferred from observation times.
        timeseries.println("4,11,12");
        timeseries.println("5,22,22");
        timeseries.close();
        timeseries_bytes.close();
        // in.println(""); end with 1 (but not more) un-terminated sequence, resulting in a warning printed to System.err

        System.setIn(new ByteArrayInputStream(timeseries_bytes.toByteArray()));

        String[] args = new String[] { dbURL };
        gov.sandia.rbb.tools.Put.main(args);

        String q = "select *, RBB_ID_TO_TAGSET(TAGSET_ID) as TAGS from RBB_EVENTS order by START_TIME;";
        H2SRBBTest.printQueryResults(rbb.db(), q);
        ResultSet rs = rbb.db().createStatement().executeQuery(q);
        assertTrue(rs.next());
        assertEquals(0.5, rs.getDouble("START_TIME"), 1e-6);
        assertEquals(3.0, rs.getDouble("END_TIME"), 1e-6);
        final long id1 = rs.getLong("ID");
        assertEquals(1L, id1);
        assertEquals("n=1,variable=x", rs.getString("TAGS")); // in particular, checkTagArray 'start' was removed.
        assertEquals(2, H2STimeseries.getNumObservations(rbb.db(), id1));
        assertTrue(rs.next());
        final long idEmpty = rs.getLong("ID");
        assertEquals(3.5, rs.getDouble("START_TIME"), 1e-6);
        assertEquals(3.5, rs.getDouble("END_TIME"), 1e-6);
        assertEquals(0, H2STimeseries.getNumObservations(rbb.db(), idEmpty));
        assertTrue(rs.next());
        assertEquals(4.0, rs.getDouble("START_TIME"), 1e-6);
        assertEquals(5.0, rs.getDouble("END_TIME"), 1e-6);
        assertTrue(rs.next());
        assertEquals(123.0, rs.getDouble("START_TIME"), 1e-6);
        assertEquals(123.0, rs.getDouble("END_TIME"), 1e-6);
        assertEquals("variable=event", rs.getString("TAGS")); // in particular, checkTagArray 'time' was removed.
        assertFalse(rs.next());

        // test the data values in sequence 1
        Object[] ids = new Object[] { id1 };
        rs = H2STimeseries.getSamples(rbb.db(), id1, null, null, 0, 0, null, null);
        //  System.err.println(H2SRBBTest.toString(rs));
        assertTrue(rs.next());
        assertEquals(1.0, rs.getDouble("TIME"), 1e-6);
        Object[] x = (Object[]) rs.getArray("SAMPLE").getArray();
        assertEquals(x.length, 2);
        assertEquals((Float)x[0], 11.0f, 1e-6f);
        assertEquals((Float)x[1], 12.0f, 1e-6f);
        assertTrue(rs.next());
        assertEquals(2.0, rs.getDouble("TIME"), 1e-6);
        x = (Object[]) rs.getArray("SAMPLE").getArray();
        assertEquals(x.length, 2);
        assertEquals((Float)x[0], 21.0f, 1e-6f);
        assertEquals((Float)x[1], 22.0f, 1e-6f);

        rbb.disconnect();
    }

    @Test
    public void testPutTimeSeriesMultiplex()
        throws Exception
    {
        System.err.println("Entering "+ java.lang.Thread.currentThread().getStackTrace()[1].getMethodName());

        final String dbURL = "jdbc:h2:mem:tptsm";
        RBB rbb = RBB.create(dbURL, null);

        ByteArrayOutputStream timeseries_bytes = new ByteArrayOutputStream();
        PrintStream timeseries = new PrintStream(timeseries_bytes, true);

        timeseries.println("two variable=x,n=2");
        timeseries.println("one variable=x,n=1");
        timeseries.println("one 1.1,111,112");
        timeseries.println("event time=-99,e=1");
        timeseries.println("two 2.1,211,212");
        timeseries.println("one 1.2,121,122");
        timeseries.println("one 1.3"); // specify end time without adding an observation.
        timeseries.println("two 2.2,221,222");
        timeseries.println("two +addedtag1=addedvalue1,addedtag2=addedvalue2"); // add tags to a previously created event.
        timeseries.println("event start=-98,end=-97,e=2"); // re-use a previous mux tag for an event.  This is allowed and results in a separate event.
        timeseries.println("one"); // close out this in to free up resources in 'put'
        timeseries.println("two 2.3,231,232");
        timeseries.println("two test=reuse"); // re-use a mux tag for a in that is open (not explicitly ended).  This is allowed and starts a new, separate in.
        timeseries.println("two 3.3,331,332");

        System.setIn(new ByteArrayInputStream(timeseries_bytes.toByteArray()));

        String[] args = new String[] { "-mux", dbURL };
        gov.sandia.rbb.tools.Put.main(args);

        String q = "select ID, START_TIME, END_TIME, RBB_ID_TO_TAGSET(TAGSET_ID) as TAGS from rbb_events order by start_time;";
        H2SRBBTest.printQueryResults(rbb.db(), q);
        ResultSet rs = rbb.db().createStatement().executeQuery(q);
        assertTrue(rs.next());   // event e=1
        assertEquals("e=1", rs.getString("TAGS")); // note the time=-99 is not stored as a tag, but rather in the START_TIME and END_TIME
        assertEquals(-99.0, rs.getDouble("START_TIME"), 1e-6);
        assertEquals(-99.0, rs.getDouble("END_TIME"), 1e-6);
        assertTrue(rs.next()); // event e=2
        assertEquals("e=2", rs.getString("TAGS")); // note the 'start' and 'end' tags are not stored in the tagset, but rather in the START_TIME and END_TIME
        assertEquals(-98.0, rs.getDouble("START_TIME"), 1e-6); 
        assertEquals(-97.0, rs.getDouble("END_TIME"), 1e-6);
        assertTrue(rs.next());
        assertEquals(1.1, rs.getDouble("START_TIME"), 1e-6); // n=1
        assertEquals(1.3, rs.getDouble("END_TIME"), 1e-6);
        long id1 = rs.getLong("ID");
        assertEquals(1L, id1);
        assertTrue(rs.next());
        assertEquals("addedtag1=addedvalue1,addedtag2=addedvalue2,n=2,variable=x", rs.getString("TAGS"));
        assertEquals(2.1, rs.getDouble("START_TIME"), 1e-6);
        assertEquals(2.3, rs.getDouble("END_TIME"), 1e-6);
        assertTrue(rs.next());
        assertEquals(3.3, rs.getDouble("START_TIME"), 1e-6);
        assertEquals(3.3, rs.getDouble("END_TIME"), 1e-6);
        assertFalse(rs.next()); // make sure there are no 'surprise' sequences

        // test the data values in sequence 1
        rs = H2STimeseries.getSamples(rbb.db(), id1, null, null, 0, 0, null, null);
        //  System.err.println(H2SRBBTest.toString(rs));
        assertTrue(rs.next());
        assertEquals(1.1, rs.getDouble("TIME"), 1e-6);
        Object[] x = (Object[]) rs.getArray("SAMPLE").getArray();
        assertEquals(x.length, 2);
        assertEquals((Float)x[0], 111.0f, 1e-6f);
        assertEquals((Float)x[1], 112.0f, 1e-6f);
        assertTrue(rs.next());
        assertEquals(1.2, rs.getDouble("TIME"), 1e-6);
        x = (Object[]) rs.getArray("SAMPLE").getArray();
        assertEquals(x.length, 2);
        assertEquals((Float)x[0], 121.0f, 1e-6f);
        assertEquals((Float)x[1], 122.0f, 1e-6f);
        assertFalse(rs.next());

        rbb.disconnect();
    }

    @Test
    public void testPutTimeSeriesMultiplexRatelimit()
        throws Exception
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering " + methodName);
        final String dbURL = "jdbc:h2:mem:"+methodName;
        RBB rbb = RBB.create(dbURL, null);

        ByteArrayOutputStream timeseries_bytes = new ByteArrayOutputStream();
        PrintStream in = new PrintStream(timeseries_bytes, true);

        in.println("x=y");
        in.println("1,1");
        in.println("1.1,1.1");
        in.println("1.9,1.9");
        in.println("2.0,2.0");
        in.println("2.1,2.1");
        in.println("");

        System.setIn(new ByteArrayInputStream(timeseries_bytes.toByteArray()));

        String[] args = new String[] { "-rateLimit", "1.0", dbURL };
        gov.sandia.rbb.tools.Put.main(args);

        String q = "select ID, START_TIME, END_TIME, RBB_ID_TO_TAGSET(TAGSET_ID) as TAGS from rbb_events order by start_time;";
        H2SRBBTest.printQueryResults(rbb.db(), q);

        Timeseries[] ts = Timeseries.findWithSamples(rbb.db());
        assertEquals(1, ts.length);
        assertEquals(1.0, ts[0].getStart(), 1e-6);
        assertEquals(2.1, ts[0].getEnd(), 1e-6); // even though the observation at 2.1 wasn't used, it still extends the event.
        assertEquals(2, ts[0].getNumSamples());

        rbb.disconnect();
    }

    @Test
    public void testPutUnique()
        throws Exception
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering " + methodName);
        final String dbURL = "jdbc:h2:mem:"+methodName;
        RBB rbb = RBB.create(dbURL, null);

        ByteArrayOutputStream timeseries_bytes = new ByteArrayOutputStream();
        PrintStream in = new PrintStream(timeseries_bytes, true);

        in.println("x=y");
        in.println("1,1");
        in.println("2,2");
        in.println("");

        in.println("x=y");
        in.println("3,3");
        in.println("");

        System.setIn(new ByteArrayInputStream(timeseries_bytes.toByteArray()));

        String[] args = new String[] { "-unique", dbURL };
        gov.sandia.rbb.tools.Put.main(args);

        String q = "select ID, START_TIME, END_TIME, RBB_ID_TO_TAGSET(TAGSET_ID) as TAGS from rbb_events order by start_time;";
        // H2SRBBTest.printQueryResults(rbb.db(), q);

        Timeseries[] ts = Timeseries.findWithSamples(rbb.db());
        assertEquals(1, ts.length);
        assertEquals(1.0, ts[0].getStart(), 1e-6);
        assertEquals(2.0, ts[0].getEnd(), 1e-6);
        assertEquals(2, ts[0].getNumSamples());

        rbb.disconnect();
    }

}
