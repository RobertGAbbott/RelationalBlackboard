/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.impl.h2.statics;

import gov.sandia.rbb.RBB;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *<pre>
 * H2SBlob allows attaching arbitrary data to an RBB Event.
 * The data is a "black box" to the RBB itself - it is not indexed by content,
 * no charset or end-of-line conversion is performed, etc.
 * Typical uses may include sounds, images, etc.  The application should use
 * a different schema for each type of information e.g. an "images" schema and
 * a "sounds" schema, which enables searching for Events with specifically those types of data attached.
 * 
 * SQL Implementation:
 * The data is stored in a table <schema>.<schema> (EVENT_ID LONG, DATA BLOB);
 *
 * Rationale:
 *
 * The getBlob API returns a single value, which assumes each event has only a single
 * blob in each schema.  This limitation asserts that blobs should be uniquely identifiable
 * by the combination of the Event tags and the schema.  If you don't like that, here
 * are some options:
 * 1) Make two separate events with different tagsets reflecting how the blobs are semantically different.
 * 2) Create two separate blob events with identical tagsets, so both will always be found together.
 * 3) Store the blobs in different schema, to reflect how they are semantically different
 * 4) Combine two or more blobs into a single blob (serializaton)
 *
 * Also it may seem odd to have a schema containing only one table with the same name as the schema.
 * This is a trivial use of sql schema, but is used because other types of data
 * attached to Events (namely Timeseries) use a separate table for each event.
 * Thus using schema to organize different types of data attached to events permits a consistent implementation.
 * </pre>
 */
public class H2SBlob {

    public static void attachBlob(Connection conn, long eventID, String schema, InputStream inputStream) throws SQLException {
        StringWriter q = new StringWriter();
        q.write("CREATE SCHEMA IF NOT EXISTS \"");
        q.write(schema);
        q.write("\"; ");

        q.write("create table if not exists \"");
        q.write(schema);
        q.write("\".\"");
        q.write(schema);
        q.write("\" (EVENT_ID BIGINT PRIMARY KEY, TIME DOUBLE, DATA BLOB); ");

        conn.createStatement().execute(q.toString());

        // attachData must be called before the insert, because attachData will
        // delete any data previosly attached to the same event in the same schema.
        // If this is not done before the insert, confusion will result, such as deleting the new attachment!
        H2SEvent.attachData(conn, eventID, schema, schema);

        q = new StringWriter();
        q.write("INSERT INTO \"");
        q.write(schema);
        q.write("\".\"");
        q.write(schema);
        q.write("\" values (?, NULL, ?);"); // NULL = do not specify a time.
        java.sql.PreparedStatement ps = conn.prepareStatement(q.toString());
        ps.setLong(1, eventID);
        ps.setBinaryStream(2, inputStream);
        ps.execute();

    }

    /**
     * Create an RBB Event with an associated blob of data.
     * @param conn
     * @param startTime
     * @param endTime
     * @param tagset
     * @param schema
     * @param inputStream
     * @return
     * @throws SQLException
     */
    public static long create(Connection conn,
        double startTime,
        double endTime,
        String tagset,
        String schema,
        InputStream inputStream) throws SQLException {

        final long id = H2SEvent.create(conn, startTime, endTime, tagset);
        attachBlob(conn, id, schema, inputStream);
        return id;
    }

    public static InputStream getBlob(Connection conn, Long eventID, String schema) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("select DATA from \""+schema+"\".\""+schema+"\" where EVENT_ID = " + eventID);
        rs.next(); // if this raises an exception, you have provided an incorrect parameter!
        return rs.getBinaryStream(1);
    }

    /**
     * Attach blob from parameters in a string array, for a command-line interface.
     */
    public static void attachBlobCLI(String[] args) throws SQLException {
        try {
           if(args.length != 3 && args.length != 4)
                throw new SQLException("Usage: attachBlob dbURL schema eventID [infile]: attach contents of stdin (or infile, if specified) to an RBB Event");
            final InputStream input = args.length == 4 ? new FileInputStream(args[3]) :  System.in;
            RBB rbb = RBB.connect(args[0]);
            H2SBlob.attachBlob(rbb.db(), Long.parseLong(args[2]), args[1], input);
            rbb.disconnect();
        } catch(FileNotFoundException e) {
            throw new SQLException(e.toString());
        }
    }

    /**
     * Retrieve blob from parameters in a string array, for a command-line interface.
     */
    public static void getBlobCLI(String[] args) throws SQLException {
        try {
           if(args.length != 3 && args.length != 4)
                throw new SQLException("Usage: getBlob dbURL schema eventID [outfile]: retrieve data attached to an RBB Event and write to stdout (or outfile, if specified)");
            final OutputStream output = args.length == 4 ? new FileOutputStream(args[3]) :  System.out;
            RBB rbb = RBB.connect(args[0]);
            final InputStream input = H2SBlob.getBlob(rbb.db(), Long.parseLong(args[2]), args[1]);
            byte[] buf = new byte[4096];
            int read;
            while ((read = input.read(buf)) != -1)
                output.write(buf, 0, read);
            rbb.disconnect();
        } catch(Exception e) {
            throw new SQLException("In getBlob: " + e.toString());
        }
    }

}
