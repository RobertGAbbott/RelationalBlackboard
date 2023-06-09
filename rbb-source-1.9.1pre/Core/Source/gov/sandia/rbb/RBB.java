package gov.sandia.rbb;

import gov.sandia.rbb.impl.h2.H2EventTCPClient;
import gov.sandia.rbb.impl.h2.H2EventTrigger;
import gov.sandia.rbb.impl.h2.statics.H2SRBB;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


public class RBB
{
    private Connection db;

    /**
     *
     * create a new RBB instance connected to the specified JDBC url.
     *
     * The URL specifies where the database will be stored on disk (or in memory)
     *
     * The name is a human-readable name used by some applications including RBB ML
     * and will be defaulted to the data/time if null.
     *
     * Raises an exception if there was already an RBB or bare H2 database at the URL
     *
     */
    public static RBB create(String url, String name) throws SQLException {
        RBB rbb = new RBB();
        if(canConnectToExistingDB(url))
            throw new SQLException("Attempt to create() new RBB, but the database already exists: "+url);
        Connection db = connectSQL(url, false);
        H2SRBB.create(db, name);
        return fromOpenRBB(db);
     }

     /**
      * Connect to an RBB previously created with create()
      */
     public static RBB connect(String url) throws SQLException {
        Connection db = connectSQL(url, true);

        ResultSet rs;
        try {
           rs = db.createStatement().executeQuery("select RBB_SCHEMA_VERSION from RBB_DESCRIPTOR");
        }
        catch(SQLException e) {
            throw new SQLException("H2RBB.connect was able to establish a SQL connection to " + url + " but failed to retrieve the RBB Schema Version.\nThis probably means the database exists but is not (yet) an RBB.");
        }

        if(!rs.next())
            throw new SQLException("H2RBB.connect: the RBB_DESCRIPTOR table exists but is empty.  That's not supposed to happen!");

        final int dbSchemaVersion = rs.getInt("RBB_SCHEMA_VERSION");
        if(dbSchemaVersion != H2SRBB.schemaVersion())
            throw new SQLException("H2RBB.connect: the version of RBB " + url + " is: " + dbSchemaVersion + ", but this code is version: " + H2SRBB.schemaVersion() + ".\n");
        //+                    "It may be possible to upgrade using UpdateSchema.");

        return fromOpenRBB(db);
     }

     /**
      * If there was no database with this url, create it.
      * If there was already a database initialized as an RBB at this url, return an rbb instance.
      *
      * Otherwise raises an exception, for example if the database exists but is not (yet) an rbb
      */
     public static RBB createOrConnect(String url) throws SQLException {
        try {
             return connect(url);
        }
        catch(SQLException e) {
            if(e.getErrorCode() == 90013) {
                System.err.println(url + " not found; will now create it.");
                return RBB.create(url, null);
            }
            else {
                throw e;
            }
        }
     }

     /**
      * make an RBB instance to reference an existing H2 Connection to an RBB DB.
      *<p>
      * This used for RBB Event Listeners, because H2 fires the trigger with
      * a DB handle that's good only in that context.
      *
      */
     public static RBB fromOpenRBB(Connection conn) {
        RBB rbb = new RBB();
        rbb.db = conn;
        return rbb;
     }

    /**
     * try to open an existing SQL database.
     * returns false if the database didn't exist, or connecting fails for whatever reason.
     */
    private static boolean canConnectToExistingDB(String url)
    {
        Connection db;
        try
        {
            db = connectSQL(url,true);
            db.close();
            return true;
        }
        catch(SQLException e)
        {
            return false;
        }
    }

    /**
     * open a SQL connection to the specified URL, using the RBB-standard username "sa" and password "x"
     * If mustAlreadyExist is true, will fail if the database didn't already exist.
     * If mustAlreadyExist is false, will create the database if necessary (unless the url provided already contains ";ifexists=true", so don't don't do that!
     *
     * This is the only place in the RBB codebase that a Connection instance is created.
     */
    private static Connection connectSQL(String url, boolean mustAlreadyExist) throws SQLException
    {
        if(mustAlreadyExist)
            url += ";ifexists=true";

        // ensure org.h2.Driver is available.
        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            throw new SQLException("ClassNotFoundException for org.h2.Driver:" + e.toString(), e);
        }

        Connection db = java.sql.DriverManager.getConnection(url, "sa", "x");
        // System.err.println("connected to " + url);
        return db;
    }



    public Connection db()
    {
        return db;
    }

    /**
     * Close the underlying db connection.
     *
     * This is the only place in the RBB codebase this occurs.
     *
     * In applications that close an RBB but keep running it is important to call
     * this; otherwise the connection may stay open and the DB files remain even
     * if no RBB instances exist.
     *
     */
    public void disconnect() throws SQLException
    {
        if (db == null)
        {
//            System.err.println("RBB.disconnect: Not disconnecting, was already closed");
            return;
        }
//        System.err.println("RBB.disconnect: Disconnecting from db " + db.toString());
        db.close();
        db = null;
    }

    private final Set<H2EventTCPClient> tcpClients = new HashSet<H2EventTCPClient>();

    /**
     * <pre>
     * Each listener will be notified through its own thread,
     * through an H2EventTCPClient (even if the server is in the same process).
     * This makes the semantics consistent whether the server is embedded or remote.
     * (If this isn't what you want, see addLocalEventListener.)
     * This means that listeners must NOT make un-coordinated access to this
     * RBB instance (or others sharing the same h2 Connection via fromOpenRBB).
     * For example if the listener executes a query during a notification it must
     * do so inside a synchronization block:
     *
     * synchronized(rbb.db()) {
     *   ...
     * }
     *
     * Moreover if the main thread calls addEventListener, it must also synchronize
     * on the db Connection if there are Listeners that may be accessing the Connection.
     *
     * </pre>
     */
    public void addEventListener(RBBEventListener listener, RBBFilter f) throws SQLException
    {
        H2EventTCPClient tcp = new H2EventTCPClient(this, listener, f);
        tcp.start();
        synchronized(tcpClients) {
            tcpClients.add(tcp);
        }
    }

    public void removeEventListener(RBBEventListener listener)
        throws SQLException
    {
        synchronized(tcpClients) {
            for (Iterator<H2EventTCPClient> i = tcpClients.iterator(); i.hasNext();) {
                H2EventTCPClient tcp = i.next();
                if(tcp.listener == listener) {
                    try
                    {
                        tcp.close();
                    }
                    catch(IOException ex)
                    {
                        System.err.println(ex);
                    }
                    i.remove();
                }
            }
        }
    }

    /**
     * Set a persistent name for the RBB.
     * Doesn't necessarily have anything to do with the pathname or filename of the database.
     */
    public void setName(String name)
        throws SQLException
    {
        H2SRBB.setName(db, name);
    }

    public String getName()
        throws SQLException
    {
        return H2SRBB.getName(db);
    }

    /**
     * listeners added this way will be notified synchronously, in the
     * thread running the database engine.
     *
     * If you call this from a client in a different process from the database
     * server, it will not be called at all.
     *
     * H2 does not allow modifying the database within this context.
     *
     * Must be removed with removeLocalEventListener.
     */
    public void addLocalEventListener(RBBEventListener listener, RBBFilter filter) throws SQLException
    {
        H2EventTrigger.addListener(H2SRBB.getUUID(db), listener, filter);
    }

    public void removeLocalEventListener(RBBEventListener listener) throws SQLException
    {
        H2EventTrigger.removeListener(H2SRBB.getUUID(db), listener);
    }

    public void printStats(PrintStream ps) throws SQLException {
        ps.println("RBB Name: " + getName());

        Statement s = db.createStatement();
        ResultSet rs = s.executeQuery("select count(*) from RBB_EVENTS");
        rs.next();
        final long numEvents = rs.getLong(1);

        rs = s.executeQuery("select count(*) from rbb_event_data where schema_name='RBB_TIMESERIES';");
        rs.next();
        final long numTimeseries = rs.getLong(1);

        ps.println(""+(numEvents-numTimeseries)+" Events (exclusive of Timeseries)");
        ps.println(""+numTimeseries+" Timeseries");

        // timeseries samples
        long totalSamples = 0;
        rs = s.executeQuery("select TABLE_NAME from information_schema.tables where table_schema='RBB_TIMESERIES';");
        Statement s2 = db.createStatement();
        while(rs.next()) {
            final String tsTable = rs.getString(1);
            ResultSet rs2 = s2.executeQuery("select count(*) from RBB_TIMESERIES."+tsTable);
            rs2.next();
            totalSamples =+ rs2.getLong(1);
        }
        ps.println(""+totalSamples+" Samples in all Timeseries");
        ps.println(""+(totalSamples/numTimeseries)+" mean Samples per Timeseries");

        rs = s.executeQuery("select count(distinct(TAGSET_ID)) from RBB_TAGSETS;");
        rs.next();
        final long numTagsets = rs.getLong(1);
        ps.println(numTagsets+" Tagsets");

        rs = s.executeQuery("select count(*) from RBB_TAGSETS;");
        rs.next();
        final long numTagsetPairs = rs.getLong(1);
        ps.println(numTagsetPairs+" total Name/Value pairs in Tagsets");
        ps.println(numTagsetPairs/numTagsets+" mean Name/Value pairs per Tagset");

        rs = s.executeQuery("select count(distinct(time_coordinate_string_id)) from RBB_TIME_COORDINATES;");
        rs.next();
        ps.println(""+rs.getLong(1)+" Time Coordinate names");

        rs = s.executeQuery("select count(*) from RBB_TIME_COORDINATES;");
        rs.next();
        ps.println(""+rs.getLong(1)+" Time Coordinate parameterizations");

    }

    public void deleteRBB()
        throws SQLException
    {
        db.createStatement().execute("DROP ALL OBJECTS DELETE FILES;");
        disconnect();
    }
}

