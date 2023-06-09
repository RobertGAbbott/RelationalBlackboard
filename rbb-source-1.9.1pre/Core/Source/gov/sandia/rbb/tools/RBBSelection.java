
package gov.sandia.rbb.tools;

import gov.sandia.rbb.Event;
import gov.sandia.rbb.RBB;
import gov.sandia.rbb.RBBEventChange.Added;
import gov.sandia.rbb.RBBEventChange.Removed;
import gov.sandia.rbb.RBBFilter;
import static gov.sandia.rbb.RBBFilter.*;
import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.EventCache;
import gov.sandia.rbb.impl.h2.statics.H2SEvent;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

/**
 *
 * These methods implement a simple convention for "Selecting" events in an RBB,
 * which is creating an event with the tag "selected=<RBBID>" identifying the event that was selected.
 * <p>
 * <b>Time</b>
 * <p>
 * GetSelectedEventIDs returns the events in order of selection.  Thus it is possible to distinguish
 * the "first," "second" and so on.  The start (and end) of a selection Event is
 * set to the system time when the selection was made.
 * <p>
 * The IDs of selected Events are from the sessionRBB, so they do NOT need to be
 * valid in the coordination RBB
 * <p>
 * RBBSelection is based on EventCache so retrieving Selection information does
 * not require a query.  Be sure to call
 * destroy() on this instance if you want the program to stop waiting for selections.
 * <p>
 * Alternately use the static methods (which could also be called through SQL),
 * however each call requires a query.
 * <p>
 * You can also override selectionChanged() to respond to selections and de-selections.
 *
 * @author rgabbot
 */
public class RBBSelection {

    RBB sessionRBB, coordinationRBB;
    private EventCache selectionCache;

    public RBBSelection(RBB sessionRBB, RBB coordinationRBB) throws SQLException {

        // override eventAdded/eventRemoved so we can notify our listener if there is one.
        selectionCache = new EventCache(coordinationRBB) {
            @Override
            public synchronized void eventAdded(RBB rbb, Added ec)
            {
                super.eventAdded(rbb, ec);
                selectionChanged(rbb, Long.parseLong(ec.event.getTagset().getValue("selected")), true);
            }

            @Override
            public synchronized void eventRemoved(RBB rbb, Removed ec)
            {
                super.eventRemoved(rbb, ec);
                selectionChanged(rbb, Long.parseLong(ec.event.getTagset().getValue("selected")), false);
            }
        };

        selectionCache.initCache(byTags(new Tagset("selected")));
        this.sessionRBB = sessionRBB;
        this.coordinationRBB = coordinationRBB;
    }

    public void disconnect() throws SQLException {
        if(selectionCache != null)
            selectionCache.disconnect();
    }

    /*
     * This call creates an RBBSelection instance that does not cache the set
     * of Selections.  This may be more efficient if you're only making a handful
     * of calls.  Also there is no need to call disconnect()
     */
    public static RBBSelection oneShot(Connection sessionRBB, Connection coordinationRBB) {
        RBBSelection rbbs = new RBBSelection();
        rbbs.sessionRBB = RBB.fromOpenRBB(sessionRBB);
        rbbs.coordinationRBB = RBB.fromOpenRBB(coordinationRBB);
        return rbbs;
    }

    /*
     * This is used by the oneShot static constructor, which then initializes
     * the internal state of the resulting instance.
     */
    private RBBSelection() {
    };

    public Long[] getSelectedEventIDs() throws SQLException {
        Event[] selectionEvents;
        if(selectionCache != null)
            selectionEvents = selectionCache.findEvents(); // get all Events from the cache - all of them are selection Events.
        else
            selectionEvents = Event.find(coordinationRBB.db(), byTags("selected"));

        ArrayList<Long> result = new ArrayList<Long>();

        for(int i = 0; i < selectionEvents.length; ++i) {
            try {
               result.add(Long.parseLong(selectionEvents[i].getTagset().getValue("selected")));
            } catch(NumberFormatException ex) {
            }
        }

        return result.toArray(new Long[0]);
    }

    /**
     * De-select all events.
     * It is not an error if there were already no selected events.
     */
    public void deselectAll() throws SQLException {
        H2SEvent.delete(coordinationRBB.db(), byTags("selected"));
    }

    /*
     * Determine whetehr the specified event is selected.
     */
    public boolean isSelected(Long ID) throws SQLException {
        Event[] s;

        RBBFilter filter = byTags("selected="+ID);

        if(selectionCache != null) {
            // This uses the EventCache of all selections, which
            // does a linear search for one with the "selected=<ID>" tag.
            // If there are a huge number of selected events (thousands?), it could
            // be faster to do a non-cached query, since in the RBB it
            // would be indexed.
            s = selectionCache.findEvents(filter);
        }
        else {
            s = Event.find(coordinationRBB.db(), filter);
        }

        return s.length > 0;
    }

    /**
     * Select the Events in the sessionDB matching the Filter.
     * If  multiple events match the filter they are selected in
     * an indeterminate order.
     */
    public void selectEvents(RBBFilter... filter) throws SQLException {
        HashSet<Long> alreadySelected = new HashSet<Long>(Arrays.asList(getSelectedEventIDs()));
        final double now = System.currentTimeMillis() / 1000.0;

        for(Event eventToSelect : Event.find(sessionRBB.db(), filter)) {
            if(alreadySelected.contains(eventToSelect.getID()))
                continue;

            Event selectionEvent = new Event(coordinationRBB.db(), now, now, new Tagset("selected="+eventToSelect.getID()));

            // if we just create the new event, this EventCache instance won't know about it until a little later,
            // so call addEvent to get it in immediately.
            if(selectionCache != null)
                selectionCache.addEvent(coordinationRBB, selectionEvent);
        }
    }

    public void unSelectEvents(RBBFilter... filter) throws SQLException {
        for(Event eventToSelect : Event.find(sessionRBB.db(), filter)) {

            // if we just create the new event, this EventCache instance won't know about it until a little later,
            // so call addEvent to get it in immediately.
            if(selectionCache != null)
                selectionCache.addEvent(coordinationRBB, selectionEvent);
        }
    }

    /**
     * Override this to respond to a change in selection.
     * ID identifies the Event being selected, 'selected' indicates
     * whether it is now being selected, or deselected.
     * There is no need to call super.selectionChanged() in the override
     * since this doesn't do anything.
     */
    public void selectionChanged(RBB rbb, Long ID, boolean selected) {
        // override to do something interesting...
    }
}
