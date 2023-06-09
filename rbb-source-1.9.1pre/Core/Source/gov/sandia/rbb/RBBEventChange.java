
package gov.sandia.rbb;

import gov.sandia.rbb.util.StringsWriter;

/**
 *
 * RBBEventChange is a class that represents information about
 * changes to events, used to implement event-driven processing.
 * <p>
 * Members are public because the class has no internal logic; it just records information.
 *
 *
 * @author rgabbot
 */
public abstract class RBBEventChange implements Cloneable {

    /**
     * Generated when an event is added to the set of interest.
     * If created==true, this is because the event was just created.
     * If created==false, the event's tagset is changing from not of interest, to being of interest.
     */
    public static class Added extends RBBEventChange implements Cloneable {
        public boolean wasCreated;
        public Added(Event event, Boolean created) {
            super(event);
            this.wasCreated = created;
        }
        @Override public void toString(StringsWriter sw) { super.toString(sw); sw.write("\t"+wasCreated); }
        @Override public void dispatch(RBB rbb, RBBEventListener listener) { listener.eventAdded(rbb, this); }
        @Override public Added clone() { return new Added(event, wasCreated); }
    }

    /**
     * Generated when the start time, end time, or tagset of a matching event is modified.
     *<p>
     * If the tagset of an existing event is modified,
     * listeners for the NEW tagset WILL be nofifed, while
     * listners for the OLD tagset will NOT be notified (if it no longer matches)... see Removed for that.
     *
     */
    public static class Modified extends RBBEventChange implements Cloneable  {
        public Modified(Event event) {
            super(event);
        }
        @Override public void dispatch(RBB rbb, RBBEventListener listener) { listener.eventModified(rbb, this); }
        @Override public Modified clone() { return new Modified(event); }
    }

    /**
     * Generated when a matching event is removed from the set of interest.
     * If deleted==true, this is because the Event was deleted.
     * If deleted==false, its tagset is being changed from previously matching to no longer matching.
     *
     * @param evt
     */
    public static class Removed extends RBBEventChange implements Cloneable {
        public Removed(Event event, Boolean deleted) {
            super(event);
            this.wasDeleted = deleted;
        }
        public boolean wasDeleted;

        @Override public void toString(StringsWriter sw) { super.toString(sw); sw.write("\t"+wasDeleted); }
        @Override public void dispatch(RBB rbb, RBBEventListener listener) { listener.eventRemoved(rbb, this); }
        @Override public Removed clone() { return new Removed(event, wasDeleted); }
    }

    /*
     * Generateed when data is added to a table owned by a matching Event.
     * (This is typically used for Timeseries, in which case data[0] is the time)
     */
    public static class DataAdded extends RBBEventChange implements Cloneable  {
        public DataAdded(Event event, String schemaName, String tableName, Object[] data) {
            super(event);
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.data = data;
        }
        public String schemaName;
        public String tableName;
        public Object[] data;

        @Override public void toString(StringsWriter sw) {
            super.toString(sw);
            sw.writeStrings("\t", schemaName, "\t", tableName, "\t");
            sw.writeJoin("\t", data);
        }

        @Override public void dispatch(RBB rbb, RBBEventListener listener) { listener.eventDataAdded(rbb, this); }
        @Override public DataAdded clone() { return new DataAdded(event, schemaName, tableName, data); }
    }

    public Event event;


    public void toString(StringsWriter sw) {
        // TODO: change use of StringsWriter to StringBuilder.
        sw.writeJoin("\t", new Object[]{getClass().getSimpleName(), event.getID(), event.getStart(), event.getEnd(), event.getTagset()});
    }

    private RBBEventChange(Event event) {
        this.event = event;
    }

    public abstract void dispatch(RBB rbb, RBBEventListener listener);

    @Override public abstract RBBEventChange clone();
}
