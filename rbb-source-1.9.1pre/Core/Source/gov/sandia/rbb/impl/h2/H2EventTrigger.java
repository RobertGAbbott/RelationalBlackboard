
package gov.sandia.rbb.impl.h2;

import gov.sandia.rbb.Event;
import gov.sandia.rbb.RBB;
import gov.sandia.rbb.RBBEventChange;
import gov.sandia.rbb.RBBEventListener;
import gov.sandia.rbb.RBBFilter;
import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.impl.h2.statics.H2SRBB;
import gov.sandia.rbb.impl.h2.statics.H2STagset;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
H2EventTrigger creates notifications for EventListeners using SQL Triggers from the H2 database.

Instances of H2EventTrigger are created by the database engine because create_rbb.sql registers them as triggers, e.g.:
CREATE TRIGGER RBB_EVENT_TRIGGER after UPDATE on RBB_EVENTS FOR EACH ROW CALL "gov.sandia.rbb.impl.h2.H2EventTrigger";

Use of triggers doesn't require an RBB to be instantiated.
 */
public class H2EventTrigger
    implements org.h2.api.Trigger
{

    /**
     * Caches the UUID for the Connection associated with this Trigger.
     * This is safe to do because H2 instantiates a new Trigger for each table for each Connection; Trigger objects are not shared between Connections.
     * So in an RBB with thousands of timeseries, tens of thousands of these could be created and never used.
     * It is initialized lazily by getRBB_UUID() so should only be retrieved through that function, not accessed directly.
     */
    private String RBB_UUID;

    private int triggerType;

    /**
     * init does not do any time-consuming work (such as a sql query) because in many cases
     * an RBB will be opened and used, but most tables will never be modifed (thus the trigger never fired).
     */
    @Override
    public void init(Connection conn,
        String schemaName,
        String triggerName,
        String tableName,
        boolean before,
        int type)
    {
        RBB_UUID = null; // trigger re-initialization on demand.

        this.triggerType = type;
//        System.err.println("init trigger " + triggerName + " on " + tableName
//            + " before=" + before + " type=" + type);
    }

    @Override
    public void close()
    {
//        System.err.println("H2EventTrigger.close");
    }

    @Override
    public void remove()
    {
        //      System.err.println("H2EventTrigger.remove");
    }
    /**
     * Send the event to all interested listeners, at most once each.
     *
     * @param rbb
     * @param evt
     * @param dataSchema
     * @param dataTable
     * @param newDataRow
     * @param prevTagset
     * @throws java.sql.SQLException
     */
    protected void fireEvent(RBB rbb, Event evt, String dataSchema, String dataTable, Object[] newDataRow, Event prevEvent) throws SQLException
    {
        Set<Registration> listeners = ListenerRegistrations.getRegistrations(getRBB_UUID(rbb));

        synchronized(listeners) {

            if (listeners.isEmpty())
            {
                // System.err.println("Not firing " + toString() + " because no listeners are registered");
                return; // there are no listeners, so we are done.
            }

            // System.err.println("Firing events - thread " + Thread.currentThread());

            HashSet<RBBEventListener > willBeNotified = new HashSet<RBBEventListener >();

            for(Registration r : listeners)
            try
            {
                if(willBeNotified.contains(r.listener))
                    continue;

                final boolean amInterested = r.interested(rbb.db(), evt);

                // prevTagset is only meaningful for UPDATEs
                // If it's null, the tagset hasn't changed since last time, so our interest state hasn't changed.
                final boolean wasInterested = prevEvent == null ? amInterested : r.interested(rbb.db(), prevEvent);

                if(!amInterested && !wasInterested)
                    continue;

                willBeNotified.add(r.listener);

                // UPDATE is the interesting case, since it can eventAdded, eventModified, or eventRemoved.
                if (this.triggerType == org.h2.api.Trigger.UPDATE)
                {
                    // there are 4 combinations wasInterested and amInterested.
                    // both can't be false or we wouldn't have got here so that leaves 3
                    if(wasInterested && amInterested)
                        new RBBEventChange.Modified(evt).dispatch(rbb, r.listener); // was changed in some way that didn't affect this listener's interest
                    // if we get here, one is true and the other is false.
                    else if(amInterested)
                        new RBBEventChange.Added(evt, false).dispatch(rbb, r.listener); // event being added, though not newly created.
                    else
                        new RBBEventChange.Removed(evt, false).dispatch(rbb, r.listener);
                }
                else if(newDataRow != null)
                    new RBBEventChange.DataAdded(evt, dataSchema, dataTable, newDataRow).dispatch(rbb, r.listener);
                else if (this.triggerType == org.h2.api.Trigger.INSERT)
                    new RBBEventChange.Added(evt, true).dispatch(rbb, r.listener);
                else if (this.triggerType == org.h2.api.Trigger.DELETE)
                    new RBBEventChange.Removed(evt, true).dispatch(rbb, r.listener);
            }
            catch (Exception e)
            {
                System.err.println("Caught error while firing " + toString()
                    + ": "
                    + e.toString());
            }

            // System.err.println("Done firing events - thread " + Thread.currentThread());
        }
    }

    @Override
    public void fire(Connection conn,
        Object[] oldRow,
        Object[] newRow)
        throws SQLException {

        if(ListenerRegistrations.isEmpty())
            return;

        Event prevEvent = null;

        // check to see if tagset has changed.
        if(this.triggerType == org.h2.api.Trigger.UPDATE) {
            // 1,2,3 are start,end,id
            for(int i = 1; i <=3 && prevEvent==null; ++i)
                if(!oldRow[i].equals(newRow[i]))
                    prevEvent = eventFromRow(conn, oldRow);
        }

        //// create an Event representing the created/changed/deleted Event.
        Object[] eventRow = (this.triggerType == org.h2.api.Trigger.DELETE ? oldRow : newRow);

        Event evt = eventFromRow(conn, eventRow);

        fireEvent(RBB.fromOpenRBB(conn), evt, null, null, null, prevEvent);
    }

    private Event eventFromRow(Connection conn, Object[] row) throws SQLException {
        return new Event((Long) row[0],
            (Double) row[1], (Double) row[2], 
            new Tagset(H2STagset.fromID(conn, (Long) row[3])));            
    }

    @Override
    public String toString()
    {
        String s = "Trigger ";
        s += (this.triggerType == org.h2.api.Trigger.INSERT ? "INSERT" : this.triggerType
            == org.h2.api.Trigger.DELETE ? "DELETE" : this.triggerType
            == org.h2.api.Trigger.UPDATE ? "UPDATE" : "(UKNOWN_TYPE)");
        return s;
    }

    static private class Registration
    {

        Registration(RBBEventListener listener, RBBFilter filter)
        {
            this.listener = listener;
            this.filter = filter;
        }

        boolean interested(Connection conn, Event e) throws SQLException { return filter.matches(conn, e); }

        RBBEventListener listener;
        RBBFilter filter;
    }

    static protected class ListenerRegistrations {

        private static final Map<String, Set<Registration>> reg = new HashMap<String, Set<Registration>>();

        /**
         * Get the set of registrations for the RBB with the specified UUID.
         * If it did not exist it is implicitly created.
         *
         * The caller must not add or remove any entries to the returned Set without synchronizing on it.
         */
        static Set<Registration> getRegistrations(String UUID) {
            synchronized(reg) {
                Set<Registration> s = reg.get(UUID);
                if(s == null) {
                    s = new HashSet<Registration>();
                    reg.put(UUID, s);
                }
                return s;
            }
        }

        static void removeIfEmpty(String UUID) {
            synchronized(reg) {
                Set<Registration> s = reg.get(UUID);
                if(s != null && s.isEmpty())
                    reg.remove(UUID);
            }
        }

        /*
         * Triggers can quickly check this.. if empty, no point continuing.
         */
        static boolean isEmpty() {
            return reg.isEmpty();
        }
    }

    public static void addListener(String UUID,
        RBBEventListener  listener,
        RBBFilter filter)
        throws java.sql.SQLException
    {
        Set<Registration> r = ListenerRegistrations.getRegistrations(UUID);
        synchronized(r) {
            r.add(new Registration(listener, filter));
        }
    }

    public static void removeListener(String UUID, RBBEventListener listener)
    {
        Set<Registration> r = ListenerRegistrations.getRegistrations(UUID);
        synchronized(r) {
            // System.err.println("Removing listener - thread " + Thread.currentThread());
            Set<Registration> goners = new HashSet<Registration>();
            for(Registration r0 : r)
                if(r0.listener == listener)
                    goners.add(r0);
            r.removeAll(goners);
        }

        ListenerRegistrations.removeIfEmpty(UUID);
    }

    private String getRBB_UUID(RBB rbb) throws SQLException {
        if(RBB_UUID == null)
            RBB_UUID = H2SRBB.getUUID(rbb.db());
        return RBB_UUID;
    }
}
