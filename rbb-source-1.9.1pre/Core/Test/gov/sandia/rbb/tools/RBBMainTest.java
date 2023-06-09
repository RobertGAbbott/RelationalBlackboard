/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.tools;

import gov.sandia.rbb.Event;
import gov.sandia.rbb.RBB;
import static gov.sandia.rbb.Tagset.TC;
import static gov.sandia.rbb.tools.RBBMain.RBBMain;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author rgabbot
 */
public class RBBMainTest {

    @Test
    public void testRBBMain() throws Throwable {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);
        final String url = "jdbc:h2:mem:"+methodName;
        RBB rbb = RBB.create(url, null);

        new Event(rbb.db(), 0.0, 1.0, TC("t=zero"));
        Event e1 = new Event(rbb.db(), 1.0, 2.0, TC("t=one"));
        Event e2 = new Event(rbb.db(), 2.0, 3.0, TC("t=two"));

        RBBMain("delete", url, "t=zero");
        Event[] events = Event.find(rbb.db());
        assertEquals(2, events.length);
        assertEquals(1.0, events[0].getStart(), 1e-6);
        assertEquals(2.0, events[1].getStart(), 1e-6);

        RBBMain("delete", url, "-id", e2.getID().toString());
        events = Event.find(rbb.db());
        assertEquals(1, events.length);
        assertEquals(1.0, events[0].getStart(), 1e-6);

        RBBMain("setTags", url, "t=one", "newtag=newvalue");
        events = Event.find(rbb.db());
        assertEquals(1, events.length);
        assertEquals("newvalue", events[0].getTagset().getValue("newtag"));

        RBBMain("setTags", url, "-id", e1.getID().toString(), "newtag=newervalue");
        events = Event.find(rbb.db());
        assertEquals(1, events.length);
        assertEquals("newervalue", events[0].getTagset().getValue("newtag"));

        rbb.disconnect();
    }

}