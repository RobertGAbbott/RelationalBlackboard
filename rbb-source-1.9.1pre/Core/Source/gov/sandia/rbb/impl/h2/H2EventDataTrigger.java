
package gov.sandia.rbb.impl.h2;

import gov.sandia.rbb.Event;
import gov.sandia.rbb.RBB;
import gov.sandia.rbb.PreparedStatementCache;
import java.sql.*;

/**
H2EventDataTrigger creates notifications for EventListeners using SQL Triggers from the H2 database.

Instances of H2EventTrigger are created by the database engine because create_rbb.sql registers them as triggers, e.g.:
CREATE TRIGGER RBB_EVENT_TRIGGER after UPDATE on RBB_EVENTS FOR EACH ROW CALL "gov.sandia.rbb.impl.h2.H2EventTrigger";

Use of triggers doesn't require an RBB to be instantiated.
 */
public class H2EventDataTrigger extends H2EventTrigger
{

    private String schemaName, tableName;

    /**
     * If this trigger is installed on a table that is shared by multiple events,
     * this is the index of the column that contains the ID of the event owning this row.
     * <p>
     * If this is non-null, then eventID is ignored.
     */
    private Integer eventIDColumnIndex;

    /**
     * If this trigger is installed on a table that is owned by a single event,
     * this is its ID.
     * <p>
     * This is initialized lazily by identify() so should only be retrieved through that function, not accessed directly.
     */
    private Long eventID;

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
        this.schemaName = schemaName;
        if(this.schemaName.equals(""))
            this.schemaName = null;
        this.tableName = tableName;

        eventID = null; // trigger re-initialization by identify()

        super.init(conn, schemaName, triggerName, tableName, before, type);
    }

    @Override
    public void fire(Connection conn,
        Object[] oldRow,
        Object[] newRow)
        throws SQLException
    {
        // System.err.println("Firing data table update on " + this.fullTableName);

        if(ListenerRegistrations.isEmpty()) // this is a key optimization for bulk inserts during non-event-driven processing, e.g. RBB put
            return;

        if(eventIDColumnIndex == null && eventID == null)
            identify(conn);

        Long currentEventID = eventID;
        if(currentEventID == null)
            currentEventID = (Long) newRow[eventIDColumnIndex];

        PreparedStatementCache.Query q = PreparedStatementCache.startQuery(conn);
        q.addAlt("SELECT ID, START_TIME, END_TIME, RBB_ID_TO_TAGSET(TAGSET_ID) as TAGS FROM RBB_EVENTS WHERE ID=", currentEventID);
        ResultSet rs = q.getPreparedStatement().executeQuery();

        if(!rs.next())
            throw new java.sql.SQLException("Error getting Event associated with table " + this.schemaName + "." + this.tableName);

        //Event event = new Event(rs.getLong(1), rs.getDouble(2), rs.getDouble(3), new TagsetCopy(H2STagset.fromResultSet(rs, "TAGS")));
        Event event = new Event(rs);

        rs.close();

        // System.err.println("fire: "+event+" "+StringsWriter.join(" ", newRow));

       // notify everybody interested in the Event
       this.fireEvent(RBB.fromOpenRBB(conn), event, this.schemaName, this.tableName, newRow, null);
    }

    /**
     * Set eventIDColumnIndex, or eventID
     */
    void identify(Connection conn) throws SQLException
    {
        if(eventIDColumnIndex == null && eventID == null) {
            String q = "SELECT EVENT_ID FROM RBB_EVENT_DATA where SCHEMA_NAME='"+schemaName+"' and TABLE_NAME = '" + tableName + "' limit 2";
            ResultSet rs = conn.createStatement().executeQuery(q);
            if (!rs.next())
                throw new SQLException("Most likely the table doesn't exist yet or doesn't belong to an event yet in RBB_EVENT_DATA");
            // Need to find index of column containing the ID of the event owning the row.
            q = "SELECT ORDINAL_POSITION-1 as INDEX FROM information_schema.columns where TABLE_SCHEMA = '"+schemaName+"' and TABLE_NAME = '"+tableName+"' and COLUMN_NAME='EVENT_ID';";
            rs = conn.createStatement().executeQuery(q);
            if(!rs.next())
                throw new SQLException("H2EventDataTrigger - RBB_EVENT_DATA says table "+tableName+" is owned by RBB but it has no EVENT_ID column.");
            eventIDColumnIndex = rs.getInt("INDEX");
        }
    }

}
