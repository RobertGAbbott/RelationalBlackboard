/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.impl.h2.statics;

import java.io.InputStream;
import java.io.IOException;
import java.sql.ResultSet;
import gov.sandia.rbb.Timeseries;
import java.io.DataInputStream;
import gov.sandia.rbb.Event;
import static gov.sandia.rbb.Tagset.TC;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;
import gov.sandia.rbb.RBB;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import org.junit.Test;
import static org.junit.Assert.*;
import static gov.sandia.rbb.RBBFilter.*;

/**
 *
 * @author rgabbot
 */
public class H2SBlobTest {

    private InputStream stringToInputStream(String s) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream os = new DataOutputStream(buf);
        os.writeUTF(s);
        return new ByteArrayInputStream(buf.toByteArray());
    }

    /**
     * Make a RBB with 2 events.
     * Each owns a table in RBB_TIMESERIES,
     * and a blob in schema1 and schema2
     */
    private RBB makeRBB() throws SQLException, UnsupportedEncodingException, IOException {
        final String callerName = java.lang.Thread.currentThread().getStackTrace()[2].getMethodName();
        RBB rbb = RBB.create("jdbc:h2:mem:"+callerName, "makeRBB");
//        RBB rbb = RBB.create("jdbc:h2:file:///tmp/"+callerName);

        // make an event that owns both individual rows (Blob) in two different schema, and a whole Table (Timeseries)
        Timeseries e1 = new Timeseries(rbb, 1, 0.0, TC("id=1"));
        e1.add(rbb, 0.0, 3.14f);
        e1.setEnd(rbb.db(), 1.0);
        H2SBlob.attachBlob(rbb.db(), e1.getID(), "schema1", stringToInputStream("Event 1, Schema 1"));
        H2SBlob.attachBlob(rbb.db(), e1.getID(), "schema2", stringToInputStream("Event 1, Schema 2"));

        // make a second event that is the same.
        Timeseries e2 = new Timeseries(rbb, 1, 1.0, TC("id=2"));
        e2.add(rbb, 1.0, 3.14f);
        e2.setEnd(rbb.db(), 2.0);
        H2SBlob.attachBlob(rbb.db(), e2.getID(), "schema1", stringToInputStream("Event 2, Schema 1"));
        H2SBlob.attachBlob(rbb.db(), e2.getID(), "schema2", stringToInputStream("Event 2, Schema 2"));

        return rbb;
    }

    @Test
    public void testCloneBlob() throws Exception {
        RBB rbb = makeRBB();

        Event[] e = Event.find(rbb.db(), byTags("id=1"));

        Long id3 = H2SEvent.clonePersistent(rbb.db(), e[0].getID(), 0.1, 1.1, "id=3");
        InputStream blob = H2SBlob.getBlob(rbb.db(), id3, "schema1");
        assertEquals("Event 1, Schema 1", new DataInputStream(blob).readUTF());

        rbb.disconnect();
    }

    @Test
    public void testInvalidDeleteData() throws Exception {
        RBB rbb = makeRBB();

        try {
            H2SEvent.deleteData(rbb.db(), null, null);
            fail("H2SEvent.deleteData doesn't allow both schema and eventID to be null");
        }
        catch(SQLException e) {

        }

        rbb.disconnect();
    }

    @Test
    public void testDeleteSchema() throws Exception {
        RBB rbb = makeRBB();

        H2SEvent.deleteData(rbb.db(), "schema1", null);

        // make sure the schema is gone.
        ResultSet rs = rbb.db().createStatement().executeQuery("SELECT count(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'schema1';");
        rs.next();
        assertEquals(0, rs.getInt(1));

        // make sure the other schema and data in them are not gone.
        Timeseries[] ts = Timeseries.findWithSamples(rbb.db());
        assertEquals(2, ts.length);
        assertEquals(1, ts[0].getNumSamples());
        assertEquals(3.14f, ts[0].value(0.0)[0], 1e-6f);

        Event[] blobs = Event.find(rbb.db(), bySchema("schema2"));
        assertEquals(2, blobs.length);
        assertEquals("Event 1, Schema 2", new DataInputStream(H2SBlob.getBlob(rbb.db(), blobs[0].getID(), "schema2")).readUTF());
        assertEquals("Event 2, Schema 2", new DataInputStream(H2SBlob.getBlob(rbb.db(), blobs[1].getID(), "schema2")).readUTF());

        rbb.disconnect();
    }


    @Test
    public void testDeleteAllDataFor1Event() throws Exception {
        RBB rbb = makeRBB();

        Event[] events = Event.find(rbb.db(), byTags("id=1"));
        H2SEvent.deleteData(rbb.db(), null, events[0].getID());

        // both events still exist.
        events = Event.find(rbb.db());
        assertEquals(2, events.length);

        // but only 1 is still a timeseries
        Timeseries[] ts = Timeseries.findWithSamples(rbb.db());
        assertEquals(1, ts.length);

        ////  make sure data from all schema for event1 are gone but are still there for event2

        // Timeseries table for event1 is gone, but table for event2 is still there.
        ResultSet rs = rbb.db().createStatement().executeQuery("select count(*) from information_schema.tables where table_schema = 'RBB_TIMESERIES';");
        rs.next();
        assertEquals(1, rs.getInt(1));

        // event2 timeseries table still there and still has data.
        assertEquals(1, H2STimeseries.getNumObservations(rbb.db(), events[1].getID()));

        // data for event2 in schema1 is still there
        rs = rbb.db().createStatement().executeQuery("select count(*) from \"schema1\".\"schema1\";");
        rs.next();
        assertEquals(1, rs.getInt(1));

        // data for event2 in schema2 is still there
        rs = rbb.db().createStatement().executeQuery("select count(*) from \"schema2\".\"schema2\";");
        rs.next();
        assertEquals(1, rs.getInt(1));

        // the RBB_EVENT_DATA table is getting updated correctly.
        // there are only 3 data ownerships
        rs = rbb.db().createStatement().executeQuery("select count(*) from RBB_EVENT_DATA;");
        rs.next();
        assertEquals(3, rs.getInt(1)); // event 1 still owns data in all 3 schema
        // and they all belong to event 2.
        rs = rbb.db().createStatement().executeQuery("select count(*) from RBB_EVENT_DATA where EVENT_ID="+events[1].getID().toString());
        rs.next();
        assertEquals(3, rs.getInt(1)); // event 1 still owns data in all 3 schema

        rbb.disconnect();
    }

    /**
     * For an event with both table and row data, delete from a table data schema.
     */
    @Test
    public void testOnlyTableDataFor1Event() throws Exception {
        RBB rbb = makeRBB();

        Event[] events = Event.find(rbb.db(), byTags("id=1"));
        H2SEvent.deleteData(rbb.db(), "RBB_TIMESERIES", events[0].getID());

        // both events still exist.
        events = Event.find(rbb.db());
        assertEquals(2, events.length);

        // both only 1 is still a timeseries
        Timeseries[] ts = Timeseries.findWithoutSamples(rbb.db());
        assertEquals(1, ts.length);

        ////  make timeseries data for event1 is gone but everything else is still there.

        // Timeseries table for event1 is gone, but table for event2 is still there.
        ResultSet rs = rbb.db().createStatement().executeQuery("select count(*) from information_schema.tables where table_schema = 'RBB_TIMESERIES';");
        rs.next();
        assertEquals(1, rs.getInt(1));

        // event2 timeseries table still there and still has data.
        assertEquals(1, H2STimeseries.getNumObservations(rbb.db(), events[1].getID()));

        // data for event1 and event2 in schema1 is still there
        rs = rbb.db().createStatement().executeQuery("select count(*) from \"schema1\".\"schema1\";");
        rs.next();
        assertEquals(2, rs.getInt(1));

        // data for event1 and event2 in schema2 is still there
        rs = rbb.db().createStatement().executeQuery("select count(*) from \"schema2\".\"schema2\";");
        rs.next();
        assertEquals(2, rs.getInt(1));

        // the RBB_EVENT_DATA table is getting updated correctly.
        // there are only 5 data ownerships
        rs = rbb.db().createStatement().executeQuery("select count(*) from RBB_EVENT_DATA;");
        rs.next();
        assertEquals(5, rs.getInt(1)); // event 1 still owns data in all 3 schema
        // 3 of which belong to event 2.
        rs = rbb.db().createStatement().executeQuery("select count(*) from RBB_EVENT_DATA where EVENT_ID="+events[1].getID().toString());
        rs.next();
        assertEquals(3, rs.getInt(1)); // event 1 still owns data in all 3 schema

        rbb.disconnect();
    }

    /**
     * For an event with both table and row data, delete from a row data schema.
     */
    @Test
    public void testOnlyRowDataFor1Event() throws Exception {
        RBB rbb = makeRBB();

        Event[] events = Event.find(rbb.db(), byTags("id=1"));
        H2SEvent.deleteData(rbb.db(), "schema1", events[0].getID());

        // both events still exist.
        events = Event.find(rbb.db());
        assertEquals(2, events.length);

        // both are still timeseries
        Timeseries[] ts = Timeseries.findWithoutSamples(rbb.db());
        assertEquals(2, ts.length);

        ////  make schema1 data for event1 is gone but everything else is still there.

        // Timeseries table is still there.
        ResultSet rs = rbb.db().createStatement().executeQuery("select count(*) from information_schema.tables where table_schema = 'RBB_TIMESERIES';");
        rs.next();
        assertEquals(1, rs.getInt(1));

        // event1 timeseries table still there and still has data.
        assertEquals(1, H2STimeseries.getNumObservations(rbb.db(), events[0].getID()));

        // event2 timeseries table still there and still has data.
        assertEquals(1, H2STimeseries.getNumObservations(rbb.db(), events[1].getID()));

        // data for event1 in schema1 is gone, but still there for event2
        rs = rbb.db().createStatement().executeQuery("select count(*) from \"schema1\".\"schema1\";");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Event 2, Schema 1", new DataInputStream(H2SBlob.getBlob(rbb.db(), events[1].getID(), "schema1")).readUTF());


        // data for event1 and event2 in schema2 is still there
        rs = rbb.db().createStatement().executeQuery("select count(*) from \"schema2\".\"schema2\";");
        rs.next();
        assertEquals(2, rs.getInt(1));

        // the RBB_EVENT_DATA table is getting updated correctly.
        // there are only 5 data ownerships
        rs = rbb.db().createStatement().executeQuery("select count(*) from RBB_EVENT_DATA;");
        rs.next();
        assertEquals(5, rs.getInt(1)); // event 1 still owns data in all 3 schema
        // 3 of which belong to event 2.
        rs = rbb.db().createStatement().executeQuery("select count(*) from RBB_EVENT_DATA where EVENT_ID="+events[1].getID().toString());
        rs.next();
        assertEquals(3, rs.getInt(1)); // event 1 still owns data in all 3 schema

        rbb.disconnect();
    }


    @Test
    public void testOverwrite() throws Exception {
        RBB rbb = makeRBB();

        Event[] events = Event.find(rbb.db());
        H2SBlob.attachBlob(rbb.db(), events[0].getID(), "schema1", stringToInputStream("Event 1, Schema 1 updated data."));

        // there are still only 2 blobs in schema 1
        ResultSet rs = rbb.db().createStatement().executeQuery("select count(*) from \"schema1\".\"schema1\";");
        rs.next();
        assertEquals(2, rs.getInt(1));

        // and the data for event1 was updated.
        assertEquals("Event 1, Schema 1 updated data.", new DataInputStream(H2SBlob.getBlob(rbb.db(), events[0].getID(), "schema1")).readUTF());

        rbb.disconnect();
    }


    @Test
    public void testCreate()
        throws Exception
    {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, "testCreate");
//        RBB rbb = RBB.create("jdbc:h2:file://tmp/db");

        ByteArrayOutputStream buf1 = new ByteArrayOutputStream();
        DataOutputStream os1 = new DataOutputStream(buf1);
        os1.writeUTF("WTF is UTF");
        os1.writeDouble(1.2345);
        final long id1 = H2SBlob.create(rbb.db(), 1, 2, "name=hi,n=1", "testBlob", new ByteArrayInputStream(buf1.toByteArray()));

        // Event 1 gets two blobs in different schema.
        ByteArrayOutputStream buf1b = new ByteArrayOutputStream();
        DataOutputStream os1b = new DataOutputStream(buf1b);
        os1b.writeInt(666);
        H2SBlob.attachBlob(rbb.db(), id1, "evil", new ByteArrayInputStream(buf1b.toByteArray()));

        ByteArrayOutputStream buf2 = new ByteArrayOutputStream();
        DataOutputStream os2 = new DataOutputStream(buf2);
        os2.writeDouble(3.1415927);
        final long id2 = H2SBlob.create(rbb.db(), 2, 3, "name=hi,n=2", "testBlob", new ByteArrayInputStream(buf2.toByteArray()));

        // create a non-blob event.
        final long id3 = H2STimeseries.start(rbb.db(), 1, 3, "name=hi,n=3");
        H2STimeseries.addSampleByID(rbb.db(), id3, 3.0, new Float[]{1.0f}, null, 4.0);
        final long id4 = H2STimeseries.start(rbb.db(), 1, 3, "name=hi,n=4");

        ResultSet rs;

        Event[] events = Event.find(rbb.db(), byTags("name=hi"));
        assertEquals(4, events.length); // finds all 4 events.

        events = Event.find(rbb.db(), byTags("name=hi"), bySchema("testBlob"));
        assertEquals(2, events.length); // just finds the testBlobs this time.

        DataInputStream is1 = new DataInputStream(H2SBlob.getBlob(rbb.db(), id1, "testBlob"));
        assertEquals("WTF is UTF", is1.readUTF());
        assertEquals(1.2345, is1.readDouble(), 1e-8);

        DataInputStream is1b = new DataInputStream(H2SBlob.getBlob(rbb.db(), id1, "evil"));
        assertEquals(666, is1b.readInt());

        // make sure deleting a blob event deletes the right data, and nothing else.
        H2SEvent.deleteByID(rbb.db(), id1);

        rs = rbb.db().createStatement().executeQuery("select count(*) from rbb_events;");
        rs.next();
        assertEquals(3, rs.getInt(1));

        rs = rbb.db().createStatement().executeQuery("select count(*) from rbb_event_data;");
        rs.next();
        assertEquals(3, rs.getInt(1));

        rs = rbb.db().createStatement().executeQuery("select count(*) from \"testBlob\".\"testBlob\";");
        rs.next();
        assertEquals(1, rs.getInt(1));

        rs = rbb.db().createStatement().executeQuery("select count(*) from \"evil\".\"evil\";");
        rs.next();
        assertEquals(0, rs.getInt(1));

        try {
            H2SBlob.getBlob(rbb.db(), id1, "testBlob");
            assertFalse(true); // shouldn't be able to get a blob for a deleted event!
        } catch(org.h2.jdbc.JdbcSQLException e) {

        }

        DataInputStream is2 = new DataInputStream(H2SBlob.getBlob(rbb.db(), id2, "testBlob"));
        assertEquals(3.1415927, is2.readDouble(), 1e-8);

        assertEquals(1.0f, H2STimeseries.value(rbb.db(), id3, 3.0, null, null)[0], 1e-6f);

        rbb.disconnect();
    }

}