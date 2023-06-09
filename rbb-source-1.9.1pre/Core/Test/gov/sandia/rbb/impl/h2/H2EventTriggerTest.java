/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.impl.h2;
import org.junit.Test;
import static org.junit.Assert.*;
import gov.sandia.rbb.*;
import static gov.sandia.rbb.Tagset.TC;
import gov.sandia.rbb.impl.h2.statics.H2SEvent;
import gov.sandia.rbb.impl.h2.statics.H2STagset;
import gov.sandia.rbb.impl.h2.statics.H2STimeseries;
import static gov.sandia.rbb.RBBFilter.*;

/**
 *
 * @author rgabbot
 */
public class H2EventTriggerTest {

  @Test
    public void testEventListener()
        throws Exception
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, "testeventListener");

        // with an empty tagset, a listener is notified of everything.
        EventCounter everything = new EventCounter(rbb, false, new Tagset());

        // make one that won't get any until near the end.
        EventCounter Q = new EventCounter(rbb, false, TC("tag=Q"));

        // make one listen for EITHER "A" or "B"
        EventCounter AorB = new EventCounter(rbb, false, TC("tag=A"), TC("tag=B"));

        new Event(rbb.db(), 0, 1, TC("tag=A"));
        everything.assertCMDA(1, 0, 0, 0);
        AorB.assertCMDA(1, 0, 0, 0);

        // if a listener matches under two tagsets, it's still only notified once
        AorB.reset();
        Event evtAandB = new Event(rbb.db(), 0, 1, TC("tag=A,tag=B"));
        AorB.assertCMDA(1,0,0, 0);

        // check removeEventListener by repeating the above, but removing the listener first.
        AorB.reset();
        rbb.removeLocalEventListener(AorB);
        rbb.db().createStatement().execute("update RBB_EVENTS set END_TIME=99 where id=" + evtAandB.getID() + ";");
        AorB.assertCMDA(0,0,0, 0);

        // it works for SQL queries that don't go through RBB
        AorB.reset();
        rbb.addLocalEventListener(AorB, byTags("tag=A"));
        rbb.addLocalEventListener(AorB, byTags("tag=B"));
        rbb.db().createStatement().execute("update RBB_EVENTS set END_TIME=99 where id=" + evtAandB.getID() + ";");
        AorB.assertCMDA(0,1,0, 0);

        // none have matched Q yet.
        Q.assertCMDA(0,0,0, 0);

        // If the tagset of an existing event is modified,
        // listeners for the NEW tagset WILL be notifed it was added, while
        // listners for the OLD tagset will be notified if it was removed.
        AorB.reset();

        H2SEvent.setTagsByID(rbb.db(), evtAandB.getID(), "tag=Q");
        Q.assertCMDAER(0,0,0,0,1,0);
        AorB.assertCMDAER(0,0,0,0,0,1);

        // we already tested create and modify, now for delete.
        Q.reset();
        H2SEvent.delete(rbb.db(), byTags("tag=Q"));
        Q.assertCMDA(0,0,1, 0);

        // a tagset with a null value permutationIsSubset all events with a matching tag name
        EventCounter sunny = new EventCounter(rbb, false, TC("day,weather=sunny")); // "day" = null, will match any day
        EventCounter sunnyTuesday = new EventCounter(rbb, false, TC("day=tuesday,weather=sunny"));
        new Event(rbb.db(), 0, 1, new Tagset("day=monday,weather=sunny"));
        new Event(rbb.db(), 0, 1, new Tagset("day=tuesday,weather=sunny"));
        new Event(rbb.db(), 0, 1, new Tagset("day=wednesday,weather=sunny"));
        new Event(rbb.db(), 0, 1, new Tagset("month=july,weather=sunny"));
        sunny.assertCMDA(3, 0, 0, 0);
        sunnyTuesday.assertCMDA(1, 0, 0, 0);

        // If an event becomes of interest when its tagset is modified after the listener is already listening,
        // the event is added (but not created), and
        // subsetquent event data additions do result in notifications.
        EventCounter moreTags = new EventCounter(rbb, false, TC("color=blue"));
        Timeseries ts = new Timeseries(rbb, 1, 0, new Tagset("type=fish"));
        ts.add(rbb, 0, 0.0f);
        moreTags.assertCMDAER(0, 0, 0, 0, 0, 0);
        H2SEvent.addTags(rbb.db(), "type=fish", "color=blue");
        ts.add(rbb, 1, 1.0f);
        moreTags.assertCMDAER(0, 0, 0, 1, 1, 0); // 1 event add for adding the tag, 1 data addition for observing 1.0f
        // If an "interesting" event's tagset is changed, but it remains of interest, a modification (and nothing else) is received.
        moreTags.reset();
        H2SEvent.addTags(rbb.db(), "type=fish", "weight=6");
        moreTags.assertCMDAER(0, 1, 0, 0, 0, 0); // 1 event add for adding the tag, 1 data addition for observing 1.0f
        // If an event is changed so it's no longer of interest, it is removed (R=1) though not deleted (D=0).
        moreTags.reset();
        H2SEvent.removeTags(rbb.db(), "color", "color");
        moreTags.assertCMDAER(0, 0, 0, 0, 0, 1);
    }

    @Test
    public void testEventDataListener()
        throws Exception
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, "testEventDataListener");

        EventCounter Acount = new EventCounter(rbb, false, TC("tag=A"));
        EventCounter Bcount = new EventCounter(rbb, false, TC("tag=B"));
        EventCounter AorBcount = new EventCounter(rbb, false, TC("tag=A"),TC("tag=B"));

        Timeseries A = new Timeseries(rbb, 2, 0, new Tagset("tag=A,name=wow"));
        Acount.assertCMDA(1, 0, 0, 0);
        Bcount.assertCMDA(0, 0, 0, 0);
        AorBcount.assertCMDA(1, 0, 0, 0);

        A.add(rbb, 1, 1.0f, 1.1f);
        A.add(rbb, 2, 3.0f, 4.1f);
        Acount.assertCMDA(1, 0, 0, 2);
        Bcount.assertCMDA(0, 0, 0, 0);
        AorBcount.assertCMDA(1, 0, 0, 2);

        rbb.disconnect();
    }
}


class EventCounter
    extends RBBEventListener.Adapter
{

    int numEventsCreated;

    int numEventsModified;

    int numEventsRemoved;

    int numDataAdded;

    int numEventsAdded;

    int numEventsDeleted;

    String name;

    public EventCounter(RBB rbb,
            boolean remote,
            Tagset... tags)
        throws Exception
    {
        name = new String();
        for (Tagset t : tags)
            this.name += t.toString() + ";";

        if(remote)
            new H2EventTCPClient(rbb, this, byTags(tags)).start();
        else {
            for (Tagset t : tags)
                rbb.addLocalEventListener(this, byTags(t));
        }
    }

    public void reset()
    {
        numDataAdded = numEventsCreated = numEventsModified = numEventsRemoved = numEventsAdded = numEventsDeleted = 0;
    }

    /**
     * assert num "C"reated, "M"odified, "D"eleted, and data "A"dded.
     */
    public void assertCMDA(
        int numEventsCreated,
        int numEventsModified,
        int numEventsDeleted,
        int numDataAdded)
    {
        assertEquals(numEventsCreated, this.numEventsCreated);
        assertEquals(numEventsModified, this.numEventsModified);
        assertEquals(numEventsDeleted, this.numEventsDeleted);
        assertEquals(numDataAdded, this.numDataAdded);
    }

    /**
     * assert num "C"reated, "M"odified, "D"eleted, data "A"dded, "E"vents added, num events "R"emoved
     */
    public void assertCMDAER(
        int numEventsCreated,
        int numEventsModified,
        int numEventsDeleted,
        int numDataAdded,
        int numEventsAdded,
        int numEventsRemoved)
    {
        assertEquals(numEventsCreated, this.numEventsCreated);
        assertEquals(numEventsModified, this.numEventsModified);
        assertEquals(numEventsDeleted, this.numEventsDeleted);
        assertEquals(numDataAdded, this.numDataAdded);
        assertEquals(numEventsAdded, this.numEventsAdded);
        assertEquals(numEventsRemoved, this.numEventsRemoved);
    }

    @Override
    public void eventAdded(RBB rbb, RBBEventChange.Added ch)
    {
        // System.err.println("Listener "+this.name+" notified event created: "+evt.toString());
        ++numEventsAdded;

        if(ch.wasCreated)
            ++numEventsCreated;
    }

    @Override
    public void eventModified(RBB rbb, RBBEventChange.Modified ch)
    {
        // System.err.println("Listener "+this.name+" notified event modified: "+evt.toString());
        ++numEventsModified;
    }

    @Override
    public void eventRemoved(RBB rbb, RBBEventChange.Removed ch)
    {
        // System.err.println("Listener "+this.name+" notified event deleted: "+evt.toString());
        ++numEventsRemoved;

        if(ch.wasDeleted)
            ++numEventsDeleted;
    }

    @Override
    public void eventDataAdded(RBB rbb, RBBEventChange.DataAdded ch)
    {
//        System.err.print("Listener " + this.name
//            + " notified of new data in table " + dataTable + " for " + evt.toString() + ":");
//        for (int i = 0; i < data.length; ++i)
//        {
//            System.err.print(" ");
//            System.err.print(data[i].toString());
//        }
//        System.err.println("");
        ++numDataAdded;
    }

}
