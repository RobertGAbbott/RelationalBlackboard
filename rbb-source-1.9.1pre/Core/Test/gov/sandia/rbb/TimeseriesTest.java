
package gov.sandia.rbb;

import java.util.ArrayList;
import gov.sandia.rbb.impl.h2.statics.H2SEvent;
import gov.sandia.rbb.impl.h2.statics.H2STimeseries;
import gov.sandia.rbb.Timeseries.Sample;
import gov.sandia.rbb.impl.h2.statics.H2STime;
import java.sql.SQLException;
import org.junit.Test;
import static org.junit.Assert.*;
import static gov.sandia.rbb.Tagset.TC;
import static gov.sandia.rbb.RBBFilter.*;

/**
 *
 * @author rgabbot
 */

public class TimeseriesTest {
    @Test
    public void testTimeseries() throws SQLException {
        Timeseries t1 = new Timeseries(1.0, 4.0, TC("n=1"), 1);

        try {
            // valueLinear raises exception when called on a timeseries with 0 elements
            t1.valueLinear(2.0);
            assertTrue(false); // shouldn't get here!
        } catch(Exception e) {
            // an exception should be raised.
        }

        // with 1 element
        t1.add(1.0, 1.0f);
        for(double t = 1.0; t <= 2.0; t += 0.5) {
            assertEquals(1.0f, t1.valueLinear(t)[0], 1e-6f);
            assertEquals(1.0f, t1.valuePrev(t)[0], 1e-6f);
        }
        
        // with 2 elements.
        t1.add(2.0, 2.0f);
        for(double t = 1.0; t <= 3.0; t += 0.5) {
            assertEquals((float)t, t1.valueLinear(t)[0], 1e-6f);
            assertEquals(Math.max(1.0f, Math.min(2.0f, (float)Math.floor(t))), t1.valuePrev(t)[0], 1e-6f);
        }
        
        // when called beyond the time extent of the Event, valueLinear returns null
        // whereas extrapolateValueLinear extrapolates.
        assertNull(t1.valueLinear(5));
        assertEquals(5f, t1.extrapolateValueLinear(5)[0], 1e-6f);

        // with more (requires binary search)
        t1.add(3.0, 1.0f);
        for(double t = 1.0; t <= 2.0; t += 0.5) {
            assertEquals((float)t, t1.valueLinear(t)[0], 1e-6f);
            assertEquals(Math.max(1.0f, Math.min(2.0f, (float)Math.floor(t))), t1.valuePrev(t)[0], 1e-6f);
        }
        for(double t = 2.0; t <= 4.0; t += 0.5) {
            assertEquals(2.0f-((float)t-2.0f), t1.valueLinear(t)[0], 1e-6f);
            assertEquals(Math.max(1.0f, Math.min(2.0f, (float)Math.ceil(2.0f-((float)t-2.0f)))), t1.valuePrev(t)[0], 1e-6f);
        }
    }

    @Test
    public void testTimeseriesCopyKeepNewest() throws SQLException {
        Timeseries t = new Timeseries(1.0, 2.0, TC("n=1"), 1);
        t.keepNewest(2);
        assertEquals(0, t.getNumSamples());

        t.add(0.0, 1.0f);
        t.keepNewest(2);
        assertEquals(1, t.getNumSamples());

        t.add(1.0, 2.0f);
        t.keepNewest(2);
        assertEquals(2, t.getNumSamples());

        t.add(2.0, 3.0f);
        t.keepNewest(2);
        assertEquals(2, t.getNumSamples());

        t.add(3.0, 4.0f);
        t.add(4.0, 5.0f);
        t.add(5.0, 6.0f);
        t.keepNewest(2);
        assertEquals(2, t.getNumSamples());
        assertEquals(4.0, t.getSample(0).getTime(), 1e-8);
        assertEquals(5.0, t.getSample(1).getTime(), 1e-8);
    }

    @Test
    public void testGet() throws SQLException {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        Timeseries red1 = new Timeseries(rbb, 1, 0.0, TC("color=red,n=1"));
        red1.add(rbb, 1.0, 10.0f);
        red1.add(rbb, 2.0, 20.0f);
        red1.add(rbb, 3.0, 40.0f);
        red1.setEnd(rbb.db(), 4.0);

        Event[] events = Event.find(rbb.db(), byTags("color=red,n=1"));
        assertEquals(events.length, 1);
        Timeseries tc = Timeseries.findWithSamples(rbb.db(), byID(events[0].getID()))[0];
        assertEquals(1, tc.getDim());
        assertEquals(3, tc.getNumSamples());

        // test what happens if we use Timeseries.getCopy on an event that is not a timeseries.
        new Event(rbb.db(), 1, 2, TC("name=testEvent"));
        events = Event.find(rbb.db(), byTags("name=testEvent"));
        assertEquals(events.length, 1);
        try {
            tc = Timeseries.getByIDWithoutSamples(rbb.db(), events[0].getID());
            assertTrue(false); // we should have got an exception 
        } catch(IllegalArgumentException e) {
            // this is expected
        }

        rbb.disconnect();
    }


    @Test
    public void testGetWithinTime() throws SQLException {

        Timeseries ts = new Timeseries(0.0, 10.0, TC("color=red,n=1"), 1);
        ts.add(1.0, 10.0f);
        ts.add(2.0, 20.0f);
        ts.add(3.0, 30.0f);
        ts.add(4.0, 40.0f);

        // no samples within specified time range
        Sample[] s = ts.getSamples(1.1, 1.2);
        assertEquals(0, s.length);

        // a time is either 1. before the first sample, 2. at a sample, 3. between two samples, or 4. after the last sample.

        double[] startTimes = { -2, 2, 2.1, 4.1 };
        double[] endTimes = { -1, 3, 3.1, 5.1 };

        for(double start : startTimes)
            for(double end : endTimes) {
                ArrayList<Double> expectedSampleTimes = new ArrayList<Double>();
                for(double i = 1; i <= 4; ++i)
                    if(i >= start && i <= end)
                        expectedSampleTimes.add(i);

                s = ts.getSamples(start, end);

                assertEquals(s.length, expectedSampleTimes.size());

                for(int i = 0; i < s.length; ++i)
                    assertEquals(s[i].getTime(), expectedSampleTimes.get(i), 1e-8);
            }

    }


    @Test
    public void testGetWithTimeConversion() throws SQLException {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        H2STime.defineCoordinate(rbb.db(), "timeCoordinate=0", 1.0, 0.0);
        H2STime.defineCoordinate(rbb.db(), "timeCoordinate=10", 1.0, -10.0);

        // we are going to do a query for 9 <= t <= 10
        
        double start=6, end=14;
        Timeseries ts1 = new Timeseries(rbb, 1, start, TC("n=1,timeCoordinate=0"));
        for(; start <= end; ++start)
            ts1.add(rbb, start, (float)start); // y=x
        ts1.setEnd(rbb.db(), end);

        start=7; end=14;
        ts1 = new Timeseries(rbb, 1, start, TC("n=2,timeCoordinate=0"));
        for(; start <= end; ++start)
            ts1.add(rbb, start, (float)start); // y=x
        ts1.setEnd(rbb.db(), end);

        // start one so late it won't be found
        start=11; end=14;
        ts1 = new Timeseries(rbb, 1, start, TC("n=3,timeCoordinate=0"));
        for(; start <= end; ++start)
            ts1.add(rbb, start, (float)start); // y=x
        ts1.setEnd(rbb.db(), end);

        // do a query relative to a timecoordinate with a base of 10
        Timeseries[] ts = Timeseries.findWithSamples(rbb.db(), byTags("n"), byTime(9-10.0, 10-10.0), withTimeCoordinate("timeCoordinate=10"));
        assertEquals("1", ts[0].getTagset().getValue("n"));
        assertEquals("2", ts[1].getTagset().getValue("n"));
        assertEquals(2, ts.length); // n=3 is not found

        // the returned Timeseries are in the output time coordinate.
        assertEquals(-4.0, ts[0].getStart(), 1e-6);

        // check samples for the n=1 timeseries.  Should be at times 6-14 inclusive (byTime does not prune the samples to the query time),
        // but with 10 subtracted from time due to time coordinate.
        assertEquals(9, ts[0].getNumSamples());
        System.err.println(ts[0]);
        for(int i = 0; i < ts[0].getNumSamples(); ++i) {
            final Sample s = ts[0].getSample(i);
            // System.err.println(s);
            Float x = 6.0f+i;
            assertEquals(x-10.0, s.getTime(), 1e-8);
            assertEquals(x, s.getValue()[0], 1e-8f);
        }

        // get a value by time in output time coordinate
        Double t = 6.1;
        assertEquals(t.floatValue(), ts[0].valueLinear(t-10.0)[0], 1e-6f);
        t = 13.9;
        assertEquals(t.floatValue(), ts[0].valueLinear(t-10.0)[0], 1e-6f);
        // whereas a query outside the valid range of time is null
        t=5.9;
        assertNull(ts[0].valueLinear(t-10.0));
        t=14.1;
        assertNull(ts[0].valueLinear(t-10.0));

    }


    @Test
    public void testTagset() throws java.sql.SQLException {
        RBB rbb = RBB.create("jdbc:h2:mem:", "testTagset");

        new Timeseries(rbb, 2, 1.0, new Tagset("variable=x"));
        new Timeseries(rbb, 2, -100.0, new Tagset("variable=y,interpolate=prev"));
        new Timeseries(rbb, 2, 3.0, new Tagset("variable=z"));

        assertEquals(2, (int) H2STimeseries.getDim(rbb.db(), 1));

        Timeseries[] ts = Timeseries.findWithSamples(rbb.db(), byTags("variable=y"));

        assertEquals(1, ts.length); // only 1 timeseries is found.

        ts[0].add(rbb, 1, 10.0f, 100.0f);
        ts[0].add(rbb, 2, 20.0f, 200.0f);

        Float[] values = ts[0].value(-99);
        assertEquals(10.0f, values[0], 1e-6f);// interpolate=prev, there is no sample previous to -99, so next is used.

//        H2SEvent.setTagsByID(rbb.db(), ts[0].getID(),
//                H2TagsetStatics.set(H2SEvent.getTagsByID(rbb.db(), ts[0].getID()), "interpolate=linear"));
        H2SEvent.setTagsByID(rbb.db(), ts[0].getID(), "variable=y,interpolate=linear");

//        H2SRBBTest.printQueryResults(rbb.db(), "select *, rbb_id_to_string(TAGSET_ID) from rbb_events;");

        // get updated Timeseries... setTagsByID updates only in database
        ts = Timeseries.findWithSamples(rbb.db(), byTags("variable=y"));

        values = ts[0].value(-99); // extrapolate before the first samples
        assertEquals(-990.0f, values[0], 1e-6);

        assertNull(ts[0].value(-101)); // try to extrapolate before the start time of the timeseries

        rbb.disconnect();

    }

    @Test
    public void testValueWithTimeCoordinate() throws java.sql.SQLException {
        RBB rbb = RBB.create("jdbc:h2:mem:", "testValueWithTimeCoordinate");

        new Timeseries(rbb, 2, 2.0, new Tagset("variable=y,interpolate=prev,timeCoordinate=millisecondsUTC"));

        Timeseries[] ts = Timeseries.findWithSamples(rbb.db(), byTags("variable=y"));

        ts[0].add(rbb, 1, 10.0f, 100.0f);
        ts[0].add(rbb, 2, 20.0f, 200.0f);

        // setup time coordinates
        H2STime.defineCoordinate(rbb.db(), "timeCoordinate=secondsUTC", 1.0, 0.0);
        H2STime.defineCoordinate(rbb.db(), "timeCoordinate=millisecondsUTC", 1000.0, 0.0);

        Float[] values = ts[0].value(2.0);
        assertEquals(10.0f, values[0], 20.0f);


        Timeseries tsMs = new Timeseries(ts[0], withTimeCoordinate("timeCoordinate=millisecondsUTC"), rbb.db());
        values = tsMs.value(2.0);
        assertEquals(10.0f, values[0], 20.0f);

        rbb.disconnect();
    }

    @Test
    public void testPromoteTimeseries() throws java.sql.SQLException {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);
        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        Event e = new Event(rbb.db(), 0.0, 0.0, new Tagset("type=test"));

        Timeseries t = new Timeseries(rbb, 2, 0.0, new Tagset("type=test"));
        t.add(rbb, 1.0, 1.0f, 11.0f);
        t.setEnd(rbb.db(), 2.0);


        Event[] events = Event.find(rbb.db());
        assertEquals(2, events.length);

        assertFalse(events[0] instanceof Timeseries);
        assertFalse(events[1] instanceof Timeseries);

        Event[] mixed = Timeseries.promoteTimeseries(rbb.db(), events);

        assertFalse(mixed[0] instanceof Timeseries);
        assertTrue(mixed[1] instanceof Timeseries); // t has later start time than e so will be element 1.

        Timeseries t2 = (Timeseries) mixed[1];

        assertFalse(t2 == t);

        assertEquals(2, t2.getDim());

        rbb.disconnect();

    }

    @Test
    public void testPersist() throws SQLException {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);
        Timeseries a = new Timeseries(1.0, 3.0, new Tagset("hi=there"), 2);
        a.add(1.1, 1.2f, 1.3f);
        a.add(2.1, 2.2f, 2.3f);

        a.persist(rbb.db());


        Timeseries b = Timeseries.getByIDWithoutSamples(rbb.db(), a.getID());
        b.loadAllSamples(rbb.db());

        assertEquals(a.getID(), b.getID());
        assertEquals(a.getNumSamples(), b.getNumSamples());
        assertEquals(a.getDim(), b.getDim());
        assertEquals(a.getSample(1).getTime(), b.getSample(1).getTime(), 1e-6);
        assertEquals(a.getSample(1).getValue()[1], b.getSample(1).getValue()[1], 1e-6f);

            
        rbb.disconnect();
    }

}
