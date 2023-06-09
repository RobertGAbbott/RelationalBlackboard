
package gov.sandia.rbb;

import gov.sandia.rbb.impl.h2.statics.H2STimeseries;
import gov.sandia.rbb.impl.h2.statics.H2STime;
import gov.sandia.rbb.impl.h2.statics.H2SEvent;
import org.junit.Test;
import static org.junit.Assert.*;
import static gov.sandia.rbb.Tagset.TC;
import static gov.sandia.rbb.RBBFilter.*;

/**
 *
 * @author rgabbot
 */

public class EventCacheTest {

    /*
     * This tests a concurrency issue with the Timeseries/Event cache, which is that
     * if you create a new Event it will not appear in the cache immediately.
     * This can be surprising if the Event and Cache are both local.
     * To remedy this you can manually use addEvent on the cache.
     */
    @Test
    public void testNewTimeseries() throws Exception {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        EventCache rbbCache = new EventCache(rbb);
        rbbCache.initCache();

        Timeseries t = new Timeseries(rbb, 1, 2.0, new Tagset("name=hi"));
        rbbCache.addEvent(rbb, t);
        // try finding the new event from the cache before we got a chance to be asynchronously notified of it.
        Timeseries[] before = rbbCache.findTimeseries();
        assertEquals(1, before.length);

        // sleep so we will be asynchronously notified.
        Thread.sleep(200);

        // by now we have been notified.  Check that this did not create a duplicate Event in the cache.
        Timeseries[] after = rbbCache.findTimeseries();
        assertEquals(1, after.length);

        rbbCache.disconnect();
        rbb.disconnect();
    }

    @Test
    public void testEventCache() throws Exception {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

//        Timeseries red1 = new Timeseries(rbb, 1, 0.0, TC("color=red,n=1"));
        Timeseries red1 = new Timeseries(rbb, 1, 0.0, TC("color=red,n=1"));
        red1.add(rbb, 1.0, 10.0f);
        red1.add(rbb, 2.0, 20.0f);
        red1.add(rbb, 3.0, 40.0f);

        Timeseries red2 = new Timeseries(rbb, 1, 1.0, TC("color=red,n=2"));
        red2.add(rbb, 1.0, 1.0f);
        red2.add(rbb, 2.0, 2.0f);
        red2.add(rbb, 3.0, 3.0f);

        Timeseries blue3 = new Timeseries(rbb, 1, 1.0, TC("color=blue,n=3"));
        blue3.add(rbb, 1.0, 1.0f);
        blue3.add(rbb, 2.0, 2.0f);

        EventCache rbbCache = new EventCache(rbb);
        rbbCache.initCache(byTags("color=red"));

        assertEquals(2, rbbCache.getNumCachedEvents());

        Timeseries red4 = new Timeseries(rbb, 1, 2.0, TC("color=red,n=4"));
        red4.add(rbb, 2.0, 2.0f);
        red4.add(rbb, 3.0, 3.0f);

        // event listener notification is asynchronous, so the cache won't receive
        // the new timeseries until a moment after it's created!
        Thread.sleep(400);
        assertEquals(3, rbbCache.getNumCachedEvents());

        Event[] eventsByID = rbbCache.getEventsByID(red1.getID(), red2.getID(), blue3.getID(), red4.getID());
        assertEquals(4, eventsByID.length);
        assertEquals(red1.getID(), eventsByID[0].getID()); // retrieves the rest in order.
        assertEquals(red2.getID(), eventsByID[1].getID());
        assertEquals(null,         eventsByID[2]); // blue is not in the red cache, so EventCache returns null in that place.
        assertEquals(red4.getID(), eventsByID[3].getID());

        H2SEvent.deleteByID(rbb.db(), red2.getID());

        Thread.sleep(100);
        assertEquals(2, rbbCache.getNumCachedEvents());

        Timeseries red1Cache = rbbCache.findTimeseries(byTags("color=red,n=1"))[0];

        assertEquals("1", red1Cache.getTagset().getValue("n"));
        assertEquals(3, red1Cache.getNumSamples());

        // make sure our cached timeseries will see the new data added.
        red1.add(rbb, 4.0, 50.0f);
        Thread.sleep(100);
        red1Cache = rbbCache.findTimeseries(byTags("color=red,n=1"))[0];
        assertEquals(4, red1.getNumSamples());
        assertEquals(4, red1Cache.getNumSamples());
        assertEquals(45.0f, red1Cache.valueLinear(3.5)[0], 1e-6f);

        // now change tags, which in this case will move from one cache to another.
        EventCache neonYellowCache = new EventCache(rbb);
        neonYellowCache.initCache(byTags("color=neonYellow"));
        assertEquals(1, rbbCache.findTimeseries(byTags("color=red,n=1")).length);
        assertEquals(0, neonYellowCache.findTimeseries(byTags("color=neonYellow")).length);
        H2SEvent.setTags(rbb.db(), "color=red,n=1", "color=neonYellow");
        Thread.sleep(100);
        assertEquals(0, rbbCache.findTimeseries(byTags("color=red,n=1")).length);
        assertEquals(1, neonYellowCache.findTimeseries(byTags("color=neonYellow")).length);

        rbbCache.disconnect();
        rbb.disconnect();
    }

    @Test
    public void testEventCacheTruncate() throws Exception {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

//        Timeseries red1 = new Timeseries(rbb, 1, 0.0, TC("color=red,n=1"));
        Timeseries red1 = new Timeseries(rbb, 1, 0.0, TC("color=red,n=1"));
        red1.add(rbb, 1.0, 10.0f);
        red1.add(rbb, 2.0, 20.0f);
        red1.add(rbb, 3.0, 30.0f);
        red1.add(rbb, 4.0, 40.0f);

        EventCache rbbCache = new EventCache(rbb);
        rbbCache.initCache(byTags("color=red"));

        Timeseries[] ta = rbbCache.findTimeseries();
        assertEquals(1, ta.length);
        Timeseries t = ta[0];
        assertEquals(4, t.getNumSamples());

        // setting the start time on a timeseries discards samples before the new start time, even in cached copies.
        H2SEvent.setStartByID(rbb.db(), red1.getID(), 1.1);
        Thread.sleep(250);
        assertEquals(3, t.getNumSamples());

        // even moving back the end time will drop samples after that time (although I can't think of a realistic use case for that...)
        H2SEvent.setEndByID(rbb.db(), red1.getID(), 3.5);
        Thread.sleep(200);
        assertEquals(2, t.getNumSamples());
        assertEquals(2.0, t.getSample(0).getTime(), 1e-8);
        assertEquals(3.0, t.getSample(1).getTime(), 1e-8);

        // If you set the end time on the instance from the cache, its change is
        // reflected immediately in the cache instance - no need for a sleep().
        // This is how you would really do it in anything other than testing code.
        t.setEnd(rbb.db(), 2.5);
        assertEquals(1, t.getNumSamples());
        assertEquals(2.0, t.getSample(0).getTime(), 1e-8);

        // make sure the cache is still using t as its instance
        ta = rbbCache.findTimeseries();
        assertEquals(1, ta.length);
        assertTrue(ta[0] == t);
     
        // and even if you set it on the local copy, the change is persistent in the RBB
        assertEquals(1, H2STimeseries.getNumObservations(rbb.db(), t.getID()));

        rbbCache.disconnect();
        rbb.disconnect();
    }

    @Test
    public void testEventCacheWithSizeLimit() throws Exception {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        Timeseries red1 = new Timeseries(rbb, 1, 0.0, TC("color=red,n=1"));
        red1.add(rbb, 1.0, 1.0f);
        red1.add(rbb, 2.0, 2.0f);
        red1.add(rbb, 3.0, 3.0f);

        Timeseries red2 = new Timeseries(rbb, 1, 1.0, TC("color=red,n=2"));
        red2.add(rbb, 1.0, 1.0f);
        red2.add(rbb, 2.0, 2.0f);
        red2.add(rbb, 3.0, 3.0f);

        Timeseries blue3 = new Timeseries(rbb, 1, 1.0, TC("color=blue,n=3"));
        blue3.add(rbb, 1.0, 1.0f);
        blue3.add(rbb, 2.0, 2.0f);

        EventCache rbbCache = new EventCache(rbb);
        rbbCache.setMaxSamples(2); /// <<<<----- set max samples (before calling initCache!)
        rbbCache.initCache(byTags("color=red"));

        assertEquals(2, rbbCache.getNumCachedEvents());

        Timeseries red4 = new Timeseries(rbb, 1, 2.0, TC("color=red,n=4"));
        red4.add(rbb, 2.0, 2.0f);
        red4.add(rbb, 3.0, 3.0f);

        // event listener notification is asynchronous, so the cache won't receive
        // the new timeseries until a moment after it's created!
        Thread.sleep(100);
        assertEquals(3, rbbCache.getNumCachedEvents());

        H2SEvent.deleteByID(rbb.db(), red2.getID());

        Thread.sleep(100);
        assertEquals(2, rbbCache.getNumCachedEvents());

        Timeseries red1Cache = rbbCache.findTimeseries(byTags("color=red,n=1"))[0];

        assertEquals("1", red1Cache.getTagset().getValue("n"));
        // make sure it's only keeping 2
        assertEquals(2, red1Cache.getNumSamples());
        // make sure it's keeping the newest
        assertEquals(2.0, red1Cache.getSample(0).getTime(), 1e-8);
        assertEquals(3.0, red1Cache.getSample(1).getTime(), 1e-8);

        // make sure our cached timeseries will see the new data added.
        red1.add(rbb, 4.0, 4.0f);
        Thread.sleep(100);
        assertEquals(2, red1Cache.getNumSamples());
        assertEquals(4, red1.getNumSamples()); // but the actual timeseries in the RBB keeps everything of course.
        // make sure it's keeping the newest
        assertEquals(3.0, red1Cache.getSample(0).getTime(), 1e-8);
        assertEquals(4.0, red1Cache.getSample(1).getTime(), 1e-8);

        rbbCache.disconnect();
        rbb.disconnect();
    }

    @Test
    public void testEventCacheWithTimeCoordinate() throws Exception {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, null);

        // setup time coordinates
        H2STime.defineCoordinate(rbb.db(), "timeCoordinate=secondsUTC", 1.0, 0.0);
        H2STime.defineCoordinate(rbb.db(), "timeCoordinate=millisecondsUTC", 1000.0, 0.0);


        Timeseries tsMs = new Timeseries(rbb, 2, 2.0, new Tagset("variable=y,timeCoordinate=millisecondsUTC"));
        tsMs.add(rbb, 1000, 10.0f, 100.0f); // Add a sample before initializing the cache so it will retrieve it through a query at initialization

        EventCache rbbCache = new EventCache(rbb);
        rbbCache.initCache(withTimeCoordinate("timeCoordinate=secondsUTC"));
        Thread.sleep(100);

        tsMs.add(rbb, 2000, 20.0f, 200.0f); // Add a sample after cache initialization so it will receive it event-driven

        // the cache was initialized withTimeCoordinate("timeCoordinate=secondsUTC") so all timeseries it provides will be in that time coordinate.
        Timeseries tsS = rbbCache.getTimeseriesByID(tsMs.getID())[0];
        assertEquals("secondsUTC", tsS.getTagset().getValue("timeCoordinate")); // the tagset is modifed to reflect the presentation time coordinate.

        // now we are about to do something weird, which is mix adding samples
        // through the EventCache (tsS) and non-cached (tsMs) right after each
        // other.  Without this sleep, tsS learns of the tsMs.add(2000) above
        // only after it has already locally processed tsS.add(3.0) i.e. 3000
        // so things get out of order.  This is a very artificial situation.
        Thread.sleep(100);

        tsS.add(rbb, 3.0, 30.0f, 300.0f); // Add a sample through the cached timeseries to ensure it will convert to the native time coordinate (millisecond) before storing.

        Thread.sleep(100);

        // check the samples as viewed through tsS
        assertEquals(3, tsS.getNumSamples());
        assertEquals(1.0, tsS.getSample(0).getTime(), 1e-6);
        assertEquals(2.0, tsS.getSample(1).getTime(), 1e-6);
        assertEquals(3.0, tsS.getSample(2).getTime(), 1e-6);
        // try interpolating a value at 1.5 s
        assertEquals(15.0f, tsS.value(1.5)[0], 1e-6f);
        assertEquals(150.0f, tsS.value(1.5)[1], 1e-6f);
        
        // tsMs is not in the EventCache so must manually refresh it.
        tsMs.loadAllSamples(rbb.db());

        assertEquals(3, tsMs.getNumSamples());
        assertEquals(1000.0, tsMs.getSample(0).getTime(), 1e-6);
        assertEquals(2000.0, tsMs.getSample(1).getTime(), 1e-6);
        assertEquals(3000.0, tsMs.getSample(2).getTime(), 1e-6); // tsS converted sample time of 3 to 3000 for us.

        rbbCache.disconnect();
        rbb.disconnect();
    }

}
