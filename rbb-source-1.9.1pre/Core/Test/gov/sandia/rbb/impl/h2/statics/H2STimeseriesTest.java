/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.impl.h2.statics;

import java.sql.SQLException;
import org.junit.Test;
import static org.junit.Assert.*;
import gov.sandia.rbb.*;
import static gov.sandia.rbb.Tagset.TC;
import java.sql.ResultSet;

/**
 *
 * @author rgabbot
 */
public class H2STimeseriesTest
{

    @Test
    public void testStartTimeSeries()
        throws Exception
    {
         final String methodName =
            java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering " + methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);
//        RBB rbb = RBB.create("jdbc:h2:/tmp/zz", null);

        ResultSet rs = null;

        // create new timeseries using add_to_timeseries with tagset.
        // also tests that specifying a minTimeDelta doesn't cause an error even if there's no previous sample.
        rs = rbb.db().createStatement().executeQuery(
            "call rbb_add_to_timeseries(('test','rob'), 1.1, (111,112), 0.05, 1.1);");

        // get its id
        rs = rbb.db().createStatement().executeQuery(
                "call rbb_find_events(null, null, null, null)");
        assertTrue(rs.next());
        final Long id = rs.getLong(1);

        // add to existing timeseries using the ID
        rs = rbb.db().createStatement().executeQuery(
            "call rbb_add_to_timeseries_by_id("+id+", 1.2, (121,122), null, null);");

        // adding to the timeseries doesn't change the end time unless the final
        // parameter is non-null
        assertEquals(1.1, H2SEvent.getEndByID(rbb.db(), id), 1e-6);

        // add to existing timeseries using the tagset
        // also make sure minTimeDelta doesn't kick in when it shouldn't.
        rs = rbb.db().createStatement().executeQuery(
            "call rbb_add_to_timeseries(('test','rob'), 1.3, (131,132), 0.05, 1.3);");

        // test filtering of minTimeDelta
        rs = rbb.db().createStatement().executeQuery(
            "call rbb_add_to_timeseries(('test','rob'), 1.4, (131,132), 1, 1.4);");

        new Timeseries(rbb, 2, 999.0, new Tagset("irrelevant=junk"));

        assertEquals(2, (int) H2STimeseries.getDim(rbb.db(), id));
        assertEquals(3, H2STimeseries.getNumObservations(rbb.db(), id));
        assertNull(H2STimeseries.getDim(rbb.db(), 666L));

        // adding to the timeseries does change the end time if the final
        // parameter is non-null
        assertEquals(1.3, H2SEvent.getEndByID(rbb.db(), id), 1e-6);

        //// valuePrev
        Float[] v = H2STimeseries.valuePrev(rbb.db(), id, 1.5, null); // after the last sample, valuePrev returns last sample.
        assertEquals(131.0f, v[0], 1e-6f);
        assertEquals(132.0f, v[1], 1e-6f);
         v = H2STimeseries.valuePrev(rbb.db(), id, 1.2, null); // at a sample, valuePrev returns it.
        assertEquals(121.0f, v[0], 1e-6f);
        assertEquals(122.0f, v[1], 1e-6f);
         v = H2STimeseries.valuePrev(rbb.db(), id, 1.25, null); // between samples, valuePrev returns the previous
        assertEquals(121.0f, v[0], 1e-6f);
        assertEquals(122.0f, v[1], 1e-6f);
        v = H2STimeseries.valuePrev(rbb.db(), id, 0.9, null); // before first sample, valuePrev returns first sample.
        assertEquals(111.0f, v[0], 1e-6f);
        assertEquals(112.0f, v[1], 1e-6f);

        //// valueLinear
        v = H2STimeseries.valueLinear(rbb.db(), id, 1.4, null); // after the last sample, valueLinear extrapolates.
        assertEquals(141.0f, v[0], 1e-6f);
        assertEquals(142.0f, v[1], 1e-6f);
         v = H2STimeseries.valueLinear(rbb.db(), id, 1.2, null); // at a sample, valueLinear returns it.
        assertEquals(121.0f, v[0], 1e-6f);
        assertEquals(122.0f, v[1], 1e-6f);
         v = H2STimeseries.valueLinear(rbb.db(), id, 1.25, null); // between samples, valueLinear interpolates
        assertEquals(126.0f, v[0], 1e-6f);
        assertEquals(127.0f, v[1], 1e-6f);
        v = H2STimeseries.valueLinear(rbb.db(), id, 1.0, null); // before first sample, valueLinear extrapolates.
        assertEquals(101.0f, v[0], 1e-6f);
        assertEquals(102.0f, v[1], 1e-6f);

        // value
        // no interpolate tag, none specified, the default is linear
        v = H2STimeseries.value(rbb.db(), id, 1.25, null, null);
        assertEquals(126.0f, v[0], 1e-6f);
        assertEquals(127.0f, v[1], 1e-6f);
        // no interpolate tag, prev specified
         v = H2STimeseries.value(rbb.db(), id, 1.25, null, "prev");
        assertEquals(121.0f, v[0], 1e-6f);
        assertEquals(122.0f, v[1], 1e-6f);
        // no interpolate tag, linear specified
        v = H2STimeseries.value(rbb.db(), id, 1.25, null, "linear");
        assertEquals(126.0f, v[0], 1e-6f);
        assertEquals(127.0f, v[1], 1e-6f);
        // interpolate=prev stored in tagset
        H2SEvent.setTagsByID(rbb.db(), id, "interpolate=prev");
         v = H2STimeseries.value(rbb.db(), id, 1.25, null, null);
        assertEquals(121.0f, v[0], 1e-6f);
        assertEquals(122.0f, v[1], 1e-6f);
        // interpolate tag present, but overridden by parameter
        v = H2STimeseries.value(rbb.db(), id, 1.25, null, "linear");
        assertEquals(126.0f, v[0], 1e-6f);
        assertEquals(127.0f, v[1], 1e-6f);

        rbb.disconnect();
    }

    @Test
    public void testValues() throws SQLException {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering " + methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        // first sample at 0.5, last at 5.5
        Timeseries ts = new Timeseries(rbb, 2, 0.5, new Tagset("type=timeseries"));
        for(int i = 5; i <= 45; i+=20)
            ts.add(rbb, i*0.1, i*0.1f, i*1.0f);
        ts.setEnd(rbb.db(), 5.5);

        Event ev = new Event(rbb.db(), 1.0, 10.0, TC("type=event"));

//        ResultSet rs = H2STimeseries.values(rbb.db(), new Object[]{ts.getID(), ev.getID()}, null, null, null, null);
        ResultSet rs = H2STimeseries.resampleValues(rbb.db(), new Object[]{ts.getID(), ev.getID()}, null, null);
        assertTrue(rs.next());

        // first row, t=2.5
        Object[] tsValues = (Object[]) rs.getArray(2).getArray();
        assertEquals(2.5, rs.getDouble(1), 1e-5); 
        assertEquals(2.5f, (Float)tsValues[0], 1e-5f);
        assertEquals(25.0f, (Float)tsValues[1], 1e-5f);

        Object evValues = rs.getArray(3);
        assertTrue(rs.wasNull());

        // second row, t=4.5
        assertTrue(rs.next());
        tsValues = (Object[]) rs.getArray(2).getArray();
        assertEquals(4.5, rs.getDouble(1), 1e-5); assertEquals(4.5f, (Float)tsValues[0], 1e-5f); assertEquals(45.0f, (Float)tsValues[1], 1e-5f);

        evValues = rs.getArray(3);
        assertTrue(rs.wasNull());

        // there are no more samples before the timeseries ended.
        assertFalse(rs.next());


        rbb.disconnect();
    }

    private void assertRowEquals(ResultSet rs, Double t, Float... values) throws SQLException {
        assertTrue(rs.next());
        assertEquals(t, rs.getDouble(1), 1e-8);
        java.sql.Array a = rs.getArray(2);
        if(a==null) {
            assertNull(values);
            return;
        }
        Object[] y = (Object[]) a.getArray();
        assertEquals(y.length, values.length);
        for(int i = 0; i < y.length; ++i)
            assertEquals(values[i], (Float)y[i], 1e-6f);
    }


    @Test
    public void testGetSamples() throws SQLException
    {
         final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering " + methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        H2STime.defineCoordinate(rbb.db(), "timeCoordinate=secondsUTC", 1.0, 0.0);
        H2STime.defineCoordinate(rbb.db(), "timeCoordinate=minutesUTC", 1/60.0, 0.0);

        Timeseries ts = new Timeseries(rbb, 1, 60, TC("timeCoordinate=minutesUTC"));
        ts.add(rbb, 1.0, 1.0f);
        ts.add(rbb, 2.0, 2.0f);
        ts.add(rbb, 3.0, 3.0f);
        ts.add(rbb, 4.0, 4.0f);
        ts.add(rbb, 5.0, 5.0f);

        //// first try without time conversions.  Samples are stored in their native time coordinate.

        // simple test
        ResultSet rs = H2STimeseries.getSamples(rbb.db(), ts.getID(), 1.5, 3.5, 0, 0, null, null);
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertTrue(rs.next());
        assertEquals(2.0, rs.getDouble("TIME"), 1e-8);
        assertEquals(2.0f, rs.getFloat("C1"), 1e-8f);
        assertTrue(rs.next());
        assertEquals(3.0, rs.getDouble("TIME"), 1e-8);
        assertEquals(3.0f, rs.getFloat("C1"), 1e-8f);
        assertTrue(!rs.next());

        // now with a couple values before
        rs = H2STimeseries.getSamples(rbb.db(), ts.getID(), 3.5, 4.5, 2, 0, null, null);
        assertTrue(rs.next());
        assertEquals(2.0, rs.getDouble("TIME"), 1e-8);
        assertTrue(rs.next());
        assertEquals(3.0, rs.getDouble("TIME"), 1e-8);
        assertTrue(rs.next());
        assertEquals(4.0, rs.getDouble("TIME"), 1e-8);
        assertTrue(!rs.next());

        // now with a null end time.
        rs = H2STimeseries.getSamples(rbb.db(), ts.getID(), 3.5, null, 0, 1, null, null);
        assertTrue(rs.next());
        assertEquals(4.0, rs.getDouble("TIME"), 1e-8);
        assertTrue(rs.next());
        assertEquals(5.0, rs.getDouble("TIME"), 1e-8);
        assertTrue(!rs.next());

        // now with a null start time, and 1 extra after.
        rs = H2STimeseries.getSamples(rbb.db(), ts.getID(), null, 1.5, 0, 1, null, null);
        assertTrue(rs.next());
        assertEquals(1.0, rs.getDouble("TIME"), 1e-8);
        assertTrue(rs.next());
        assertEquals(2.0, rs.getDouble("TIME"), 1e-8);
        assertTrue(!rs.next());

        // now with a couple values after
        rs = H2STimeseries.getSamples(rbb.db(), ts.getID(), 1.5, 2.5, 0, 2, null, null);
        assertTrue(rs.next());
        assertEquals(2.0, rs.getDouble("TIME"), 1e-8);
        assertTrue(rs.next());
        assertEquals(3.0, rs.getDouble("TIME"), 1e-8);
        assertTrue(rs.next());
        assertEquals(4.0, rs.getDouble("TIME"), 1e-8);
        assertTrue(!rs.next());

        // now with time conversion.  3 mins = 180 seconds
        rs = H2STimeseries.getSamples(rbb.db(), ts.getID(), 180.0, 180.0, 0, 0, "timeCoordinate=secondsUTC", null);
        assertTrue(rs.next());
        assertEquals(3.0*60, rs.getDouble("TIME"), 1e-8);
        assertEquals(3.0f, rs.getFloat("C1"), 1e-8f);
        assertTrue(!rs.next());

        // get 1 before an empty time interval
        rs = H2STimeseries.getSamples(rbb.db(), ts.getID(), 2.5, 2.5, 1, 0, null, null);
        assertTrue(rs.next());
        assertEquals(2.0, rs.getDouble("TIME"), 1e-8);
        assertTrue(!rs.next());

        // get 1 after an empty time interval
        rs = H2STimeseries.getSamples(rbb.db(), ts.getID(), 2.5, 2.5, 0, 1, null, null);
        assertTrue(rs.next());
        assertEquals(3.0, rs.getDouble("TIME"), 1e-8);
        assertTrue(!rs.next());

        rbb.disconnect();
    }

    @Test
    public void testResampleValues() throws SQLException
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering " + methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        // first sample at 0.1, last at 5.5; x=t, y=10*t
        Timeseries ts = new Timeseries(rbb, 2, 0.1, new Tagset("name=test"));
        ts.add(rbb, 0.5, 0.5f, 5.0f);
        ts.add(rbb, 2.5, 2.5f, 25.0f);
        ts.add(rbb, 5.5, 5.5f, 55.0f);
        ts.setEnd(rbb.db(), 5.6);

        ResultSet rs = H2STimeseries.resampleValues(rbb.db(), new Long[]{ts.getID()}, 0.0, 1.0, 7, null);

        assertRowEquals(rs, 0.0, (Float[])null);
        assertRowEquals(rs, 1.0, 1.0f, 10.0f);
        assertRowEquals(rs, 2.0, 2.0f, 20.0f);
        assertRowEquals(rs, 3.0, 3.0f, 30.0f);
        assertRowEquals(rs, 4.0, 4.0f, 40.0f);
        assertRowEquals(rs, 5.0, 5.0f, 50.0f);
        assertRowEquals(rs, 6.0, (Float[])null);
        assertFalse(rs.next());

        // try extrapolating a value that is within the lifespan of the Timeseries, but before the first Sample.
        rs = H2STimeseries.resampleValues(rbb.db(), new Long[]{ts.getID()}, 0.15, 1.0, 1, null);
        assertRowEquals(rs, 0.15, 0.15f, 1.5f);

        // try extrapolating a value that is within the lifespan of the Timeseries, but after the last Sample.
        rs = H2STimeseries.resampleValues(rbb.db(), new Long[]{ts.getID()}, 5.55, 1.0, 1, null);
        assertRowEquals(rs, 5.55, 5.55f, 55.5f);


//        // try a timestep big enough that not all samples are used.
        rs = H2STimeseries.resampleValues(rbb.db(), new Long[]{ts.getID()}, 0.0, 2.1, 3, null);
        assertRowEquals(rs, 0.0, (Float[])null);
        assertRowEquals(rs, 2.1, 2.1f, 21.0f);
        assertRowEquals(rs, 4.2, 4.2f, 42.0f);

        // now try with a timeseries that changes slope
        ts = new Timeseries(rbb, 1, 0.0, new Tagset("name=test"));
        ts.add(rbb, 0.0, 0.0f);
        ts.add(rbb, 1.0, 1.0f);
        ts.add(rbb, 2.0, 0.0f);
        ts.setEnd(rbb.db(), 2.0);

        rs = H2STimeseries.resampleValues(rbb.db(), new Long[]{ts.getID()}, 0.0, 1.0, 3, null);
        assertRowEquals(rs, 0.0, 0.0f);
        assertRowEquals(rs, 1.0, 1.0f);
        assertRowEquals(rs, 2.0, 0.0f);
        assertFalse(rs.next());

        rs = H2STimeseries.resampleValues(rbb.db(), new Long[]{ts.getID()}, 0.1, 1.0, 3, null);
        assertRowEquals(rs, 0.1, 0.1f);
        assertRowEquals(rs, 1.1, 0.9f);
        assertRowEquals(rs, 2.1, (Float[])null);
        assertFalse(rs.next());

        rs = H2STimeseries.resampleValues(rbb.db(), new Long[]{ts.getID()}, -0.1, 1.0, 3, null);
        assertRowEquals(rs, -0.1, (Float[])null);
        assertRowEquals(rs, 0.9, 0.9f);
        assertRowEquals(rs, 1.9, 0.1f);
        assertFalse(rs.next());

        /// now make a timeseries that has just 1 observation
        ts = new Timeseries(rbb, 1, 0.7, new Tagset("name=test3"));
        ts.add(rbb, 1.8, 99.0f);
        ts.setEnd(rbb.db(), 3.1f);

        rs = H2STimeseries.resampleValues(rbb.db(), new Long[]{ts.getID()}, 0.0, 1.0, 5, null);
        assertRowEquals(rs, 0.0, (Float[])null);
        assertRowEquals(rs, 1.0, 99.0f);
        assertRowEquals(rs, 2.0, 99.0f);
        assertRowEquals(rs, 3.0, 99.0f);
        assertRowEquals(rs, 4.0, (Float[])null);

        // test via SQL
        rs = rbb.db().createStatement().executeQuery("call RBB_RESAMPLE_TIMESERIES_VALUES(("+ts.getID()+"), 0.0, 1.0, 5, null);");
        assertRowEquals(rs, 0.0, (Float[])null);
        assertRowEquals(rs, 1.0, 99.0f);
        assertRowEquals(rs, 2.0, 99.0f);
        assertRowEquals(rs, 3.0, 99.0f);
        assertRowEquals(rs, 4.0, (Float[])null);

        // test with time coordinates
        H2STime.defineCoordinate(rbb.db(), "timeCoordinate=secondsUTC", 1.0, 0.0);
        H2STime.defineCoordinate(rbb.db(), "timeCoordinate=minutesUTC", 1/60.0, 0.0);
        ts = new Timeseries(rbb, 1, 1.0, new Tagset("name=test4,timeCoordinate=minutesUTC"));
        ts.add(rbb, 2.0, 2.0f);
        ts.add(rbb, 3.0, 3.0f);
        ts.setEnd(rbb.db(), 3.1f);
        // 1 minute = 60 seconds
        rs = H2STimeseries.resampleValues(rbb.db(), new Long[]{ts.getID()}, 59.0, 1.0, 2, "timeCoordinate=secondsUTC");
        assertRowEquals(rs, 59.0, (Float[])null); // just before 1 minute, when the timeseries starts.
        assertRowEquals(rs, 60.0, 1.0f);

        rbb.disconnect();
    }

    @Test
    public void testFindTimeseries() throws SQLException
    {
        try {
        final String methodName =
        java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering " + methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);
//        RBB rbb = RBB.create("jdbc:h2:file:/tmp/db2");

        // make an rbb with both timeseries and non-timeseries events,
        // and verify findTimeseries only returns the timeSeries!

        H2SEvent.create(rbb.db(), 1.0, 100.0, "n=1");
        final long ts1 = H2STimeseries.start(rbb.db(), 1, 2.0, "n=2");
        H2SEvent.create(rbb.db(), 3.0, 100.0, "n=3");
        final long ts2 = H2STimeseries.start(rbb.db(), 1, 4.0, "n=4");

        //// this version calls the code directly.  Call through SQL instead to test the bindings also.
        // ResultSet rs = H2STimeseries.find(rbb.db(), null, null, null, null);
        ResultSet rs = rbb.db().createStatement().executeQuery(
            "call rbb_find_timeseries(null, null, null, null, null);");

        assertTrue(rs.next());
        assertEquals(ts1, rs.getLong("ID"));
        assertTrue(rs.next());
        assertEquals(ts2, rs.getLong("ID"));
        assertFalse(rs.next()); // Events that are not timeseries should not be returned!

        // try again in combination with a tagset.
        rs = rbb.db().createStatement().executeQuery(
            "call rbb_find_timeseries('n=4', null, null, null, null);");
        assertTrue(rs.next());
        assertEquals(ts2, rs.getLong("ID"));
        assertFalse(rs.next()); // Events that are not timeseries should not be returned!

        rbb.disconnect();
        } catch (SQLException e) {
            System.err.println(e.toString());
            throw e;
        }
    }

    @Test
    public void testFindNearest() throws SQLException
    {
         final String methodName =
            java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering " + methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        // create timeseries with constant values at 0, 1, and x.
        Timeseries ts = new Timeseries(rbb, 1, 0.0, new Tagset("value=0,timeCoordinate=UTC"));
        ts.add(rbb, 0.0, 0.0f);
        ts.add(rbb, 1.0, 0.0f);
        ts.setEnd(rbb.db(), 1.0);

        ts = new Timeseries(rbb, 1, 0.0, new Tagset("value=1,timeCoordinate=UTC"));
        ts.add(rbb, 0.0, 1.0f);
        ts.add(rbb, 1.0, 1.0f);
        ts.setEnd(rbb.db(), 1.0);

        ts = new Timeseries(rbb, 1, 0.0, new Tagset("value=x,timeCoordinate=UTC"));
        ts.add(rbb, 0.0, 0.0f);
        ts.add(rbb, 1.0, 1.0f);
        ts.setEnd(rbb.db(), 1.0);

        ResultSet rs = H2STimeseries.findNearest(rbb.db(), "value", new Object[]{0.8}, 0.5, null, 99.0, 99);
        // ID, START_TIME, END_TIME, TAGS, DATA_SCHEMA, DATA_TABLE, X, DIST
        assertTrue(rs.next());
        assertEquals("1", new Tagset(rs.getString("TAGS")).getValue("value"));
        assertEquals((Float)((Object[])rs.getArray("X").getArray())[0], 1.0f, 1e-6f); // nearest to 0.8 is 1.0
        assertEquals(rs.getDouble("DIST"), 0.2, 1e-6); // nearest to 0.8 is 1.0

        assertTrue(rs.next());
        assertTrue(rs.next());
        assertFalse(rs.next());

        // use a null for max radius.
        rs = H2STimeseries.findNearest(rbb.db(), "value", new Object[]{0.8}, 0.5, null, null, 99);
        assertTrue(rs.next());
        assertEquals("1", new Tagset(rs.getString("TAGS")).getValue("value"));
        assertEquals((Float)((Object[])rs.getArray("X").getArray())[0], 1.0f, 1e-6f); // nearest to 0.8 is 1.0
        assertEquals(rs.getDouble("DIST"), 0.2, 1e-6); // nearest to 0.8 is 1.0

        assertTrue(rs.next());
        assertTrue(rs.next());
        assertFalse(rs.next());

        // use a null for max num results.
        rs = H2STimeseries.findNearest(rbb.db(), "value", new Object[]{0.8}, 0.5, null, 99.0, null);
        assertTrue(rs.next());
        assertEquals("1", new Tagset(rs.getString("TAGS")).getValue("value"));
        assertEquals((Float)((Object[])rs.getArray("X").getArray())[0], 1.0f, 1e-6f); // nearest to 0.8 is 1.0
        assertEquals(rs.getDouble("DIST"), 0.2, 1e-6); // nearest to 0.8 is 1.0

        assertTrue(rs.next());
        assertTrue(rs.next());
        assertFalse(rs.next());

        // now try the same but limited to two results.
        rs = H2STimeseries.findNearest(rbb.db(), "value", new Object[]{0.8}, 0.5, null, 99.0, 2);
        assertTrue(rs.next());
        assertEquals("1", new Tagset(rs.getString("TAGS")).getValue("value"));
        assertEquals((Float)((Object[])rs.getArray("X").getArray())[0], 1.0f, 1e-6f); // nearest to 0.8 is 1.0
        assertEquals(rs.getDouble("DIST"), 0.2, 1e-6); // nearest to 0.8 is 1.0

        assertTrue(rs.next());
        assertFalse(rs.next());

        // now try the same but limited to a radius of 0.5, which admits 1.0 (distance 0.2) and 0.5 (distance 0.3)
        rs = H2STimeseries.findNearest(rbb.db(), "value", new Object[]{0.8}, 0.5, null, 0.5, 99);
        assertTrue(rs.next());
        assertEquals("1", new Tagset(rs.getString("TAGS")).getValue("value"));
        assertEquals((Float)((Object[])rs.getArray("X").getArray())[0], 1.0f, 1e-6f); // nearest to 0.8 is 1.0
        assertEquals(rs.getDouble("DIST"), 0.2, 1e-6); // nearest to 0.8 is 1.0
        assertTrue(rs.next());
        assertFalse(rs.next());

        // now try the same but with a time coordinate.
        // at 900 ms the closest thing to 0.8 is 0.9 and the next closest is 1.0
        H2STime.defineCoordinate(rbb.db(), "timeCoordinate=UTC", 1, 0);
        H2STime.defineCoordinate(rbb.db(), "timeCoordinate=ms", 1000, 0);
        rs = H2STimeseries.findNearest(rbb.db(), "value", new Object[]{0.8}, 900, "timeCoordinate=ms", 0.5, 99);
        assertTrue(rs.next());
        assertEquals("x", new Tagset(rs.getString("TAGS")).getValue("value"));
        assertEquals((Float)((Object[])rs.getArray("X").getArray())[0], 0.9f, 1e-6f); // nearest to 0.8 is 0.9
        assertEquals(rs.getDouble("DIST"), 0.1, 1e-6); // distance from 0.8 to 0.9 is 0.1
        assertTrue(rs.next());
        assertEquals("1", new Tagset(rs.getString("TAGS")).getValue("value"));
        assertEquals((Float)((Object[])rs.getArray("X").getArray())[0], 1.0f, 1e-6f); // second nearest to 0.8 is 1.0
        assertEquals(rs.getDouble("DIST"), 0.2, 1e-6); // distance from 0.8 to 1.0 is 0.2
        assertFalse(rs.next());

        // at 100 ms the closest thing to 0.2 is 0.1 and the next closest is 0.0


        rbb.disconnect();

    }


    /*
     * Updating the start or end time of a timeseseries discards
     * Samples outside the new time range.
     */
    @Test
    public void testDeleteSamplesBeforeOrAfter() throws SQLException
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering " + methodName);
        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        Timeseries ts = new Timeseries(rbb, 1, 0.5, new Tagset("n=1"));
        ts.add(rbb, 1.0, 0.0f);
        ts.add(rbb, 2.0, 0.0f);
        ts.add(rbb, 3.0, 0.0f);
        ts.setEnd(rbb.db(), 3.5);

        Long tsID2 = H2SEvent.clonePersistent(rbb.db(), ts.getID(), null, null, "n=2");
        Long tsID3 = H2SEvent.clonePersistent(rbb.db(), ts.getID(), null, null, "n=3");
        Long tsID4 = H2SEvent.clonePersistent(rbb.db(), ts.getID(), null, null, "n=4");
        Long tsID5 = H2SEvent.clonePersistent(rbb.db(), ts.getID(), null, null, "n=4");

        H2SEvent.setByID(rbb.db(), ts.getID(), 0.5, null, null); // no samples were before this new start time so none are dropped.
        Timeseries t = Timeseries.getByIDWithoutSamples(rbb.db(), ts.getID());
        t.loadAllSamples(rbb.db());
        assertEquals(ts.getNumSamples(), t.getNumSamples());

        H2SEvent.setByID(rbb.db(), tsID2, 2.1, null, null); // trim start - t=1, t=2 were before 2.1
        t = Timeseries.getByIDWithoutSamples(rbb.db(), tsID2);
        t.loadAllSamples(rbb.db());
        assertEquals(1, t.getNumSamples());
        assertEquals(3.0, t.getSample(0).getTime(), 1e-8);

        H2SEvent.setByID(rbb.db(), tsID3, null, 1.5, null); // trim end - t=2, t=3 were after 1.5
        t = Timeseries.getByIDWithoutSamples(rbb.db(), tsID3);
        t.loadAllSamples(rbb.db());
        assertEquals(1, t.getNumSamples());
        assertEquals(1.0, t.getSample(0).getTime(), 1e-8);

        H2SEvent.setByID(rbb.db(), tsID4, 1.5, 2.5, null); // trim start and end
        t = Timeseries.getByIDWithoutSamples(rbb.db(), tsID4);
        t.loadAllSamples(rbb.db());
        assertEquals(1, t.getNumSamples());
        assertEquals(2.0, t.getSample(0).getTime(), 1e-8);


    }
}
