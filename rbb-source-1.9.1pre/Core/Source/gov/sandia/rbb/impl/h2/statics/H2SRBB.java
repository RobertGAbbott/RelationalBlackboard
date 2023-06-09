
package gov.sandia.rbb.impl.h2.statics;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.*;
import java.sql.SQLException;
import java.text.SimpleDateFormat;

/**
* Public static functions in this class implement the SQL interface to the RBB.
* The create_rbb.sql script creates aliases for these so they can be called through SQL.
* The public functions here only accept / return datatypes for which H2 has a SQL mapping.
* Most functionality is placed here, with the RBB interface implementation (H2RBB) being a thin wrapper over them.
 */
public class H2SRBB
{
    /**
     * Runs the create_rbb.sql script on an existing database (passed in through conn).
     * Creates the tables and aliases, and sets the human-readable name.
     * name may be null and will be defaulted to the date/time of creation.
     * @param conn Sql connection.
     * @throws java.sql.SQLException
     */
    public static void create(Connection conn, String name)
        throws java.sql.SQLException
    {
        final String scriptname = "/gov/sandia/rbb/impl/h2/resources/create_rbb.sql";
        java.io.InputStream script = conn.getClass().getResourceAsStream(scriptname);
        if (script == null)
            System.err.println("Failed to find " + scriptname);
        // System.err.println("Starting execution of create_rbb.sql");
        org.h2.tools.RunScript.execute(conn, new java.io.InputStreamReader(
            script));

        if(name == null)
            name = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new Date());
        setName(conn, name);
       //  System.err.println("Executed creation script");
    }

    /**
     * Retrieves the UUID that uniquely identifies this RBB
     * @param conn
     * @return
     * @throws java.sql.SQLException
     */
    public static String getUUID(Connection conn)
        throws java.sql.SQLException
    {
        ResultSet rs = conn.createStatement().executeQuery(
            "select RBB_UUID from RBB_DESCRIPTOR");
        if (!rs.next())
            throw new java.sql.SQLException("failed to retrieve RBB UUID!");
        return rs.getString(1);
    }

    /**
     * Returns the RBB database schema version of the RBB java code.
     * This should be incremented whenever the RBB schema changes.
     * When an RBB is created, it is stamped with the schemaVersion of the
     * code used to create it.
     * When an RBB is opened, the schema version of the code must match the
     * one stored in the RBB.
     */
    public static int schemaVersion() {
        return 9;
    }

    /**
     *
     * H2 doesn't seem to have a way to return the maximum double value, so make one.
     * We really might prefer POSITIVE_INFINITY to MAX_VALUE, but POSITIVE_INFINITY comes out as the string "Infinity" in SQL which has no numeric meaning.
     * */
    public static double maxDouble()
    {
        return java.lang.Double.MAX_VALUE;
    }

        public static long nextID(Connection conn)
        throws java.sql.SQLException
    {
        ResultSet rs = conn.createStatement().executeQuery(
            "call nextval('RBB_ID');");
        if (!rs.next())
        {
            throw new java.sql.SQLException("Error creating RBB_ID");
        }
        return rs.getLong(1);

    }
        
    public static void setName(Connection conn, String name) throws java.sql.SQLException
    {
        java.sql.PreparedStatement ps = conn.prepareStatement("update rbb_descriptor set rbb_name=?");
        ps.setString(1, name);
        ps.execute();
    }

    public static String getName(Connection conn) throws java.sql.SQLException
    {
        ResultSet rs = conn.createStatement().executeQuery(
            "select RBB_NAME from RBB_DESCRIPTOR");
        if (!rs.next())
        {
            throw new java.sql.SQLException("failed to retrieve RBB_NAME!");
        }
        return rs.getString(1);
    }

    /**
     * Start the Event TCP Server and return the port number it is listening on.
     *
     */
    public static int startEventTCPServer(Connection conn) throws SQLException {
        // Call through a query.  Directly calling H2EventTCPServer.start();
        // Calling this directly in a client process would start the TCP server in
        // the client process instead of the server process,
        // which doesn't work (since H2 triggers are called in the server process.)
        ResultSet rs = conn.createStatement().executeQuery("call rbb_start_event_tcp_server();");
        rs.next();
        return rs.getInt(1);
    }

    /**
     * Stop the Event TCP Server.
     *
     */
    public static void stopEventTCPServer(Connection conn) throws SQLException {
        // Call through a query because this must be run server-side, as with startEventTCPServer().
        conn.createStatement().executeQuery("call rbb_stop_event_tcp_server();");
    }

    /**
     * Get the name of the host on which the server is running.
     */
    public static String getServerAddress(Connection conn) throws SQLException {
        /**
         * do a query so we know we get the hostname of the server, not the client.
         */
        ResultSet rs = conn.createStatement().executeQuery("call RBB_GET_SERVER_ADDRESS();");
        rs.next();
        String serverAddress = rs.getString(1);

//        if(serverAddress.equals(privateGetServerAddress()))
//            return "127.0.0.1";

        return serverAddress;
    }

    /**
     * This is public so the H2 database engine can call it, but you should
     * call getServerAddress instead, so you get the hostname of the server and not the client!
     */
    public static String privateGetServerAddress() throws SQLException {
        try {

            // the following two lines just randomly chooses an address, which may be 127.0.0.1, which is not good.
            //  InetAddress addr = InetAddress.getLocalHost();
            //  return addr.getHostAddress();

            ArrayList<String> loopbackAddresses = new ArrayList<String>();
            ArrayList<String> siteLocalAddresses = new ArrayList<String>();
            ArrayList<String> otherAddresses = new ArrayList<String>();

            Enumeration<NetworkInterface> netInterfaces=NetworkInterface.getNetworkInterfaces();
            while(netInterfaces.hasMoreElements()){
                    NetworkInterface ni = netInterfaces.nextElement();
                    Enumeration<InetAddress> inetAddresses = ni.getInetAddresses();
					while(inetAddresses.hasMoreElements()) {
                        InetAddress ip = inetAddresses.nextElement();
                        // System.err.println(ip.getHostAddress() + " on " + ni.getDisplayName());
                        if(ip.isLoopbackAddress())
                            loopbackAddresses.add(ip.getHostAddress());
                        else if(ip.isSiteLocalAddress())
                            siteLocalAddresses.add(ip.getHostAddress());
                        else if(ip.getHostAddress().indexOf(":")==-1) // ":" to filter out MAC addresses that are sometimes returned.  Odd.
                            otherAddresses.add(ip.getHostAddress());
                }
            }

            String result = null;

            if(!otherAddresses.isEmpty())
                result = otherAddresses.get(0);
            else if(!siteLocalAddresses.isEmpty())
                result = siteLocalAddresses.get(0);
            else if(!loopbackAddresses.isEmpty())
                result = loopbackAddresses.get(0);

            // System.err.println("returning " + result);

            return result;

            // return addr.getHostName();
        } catch (Exception e) {
            throw new SQLException("H2SRBB.getHostnameImpl gotException: " + e.toString());
        }
    }

    private final static Set<String> localRBBs = new HashSet<String>();

    /**
     * This is an implementation method called by RBB itself.
     */
    public static void privateSetLocal(Connection conn) throws SQLException {
        synchronized(localRBBs) {
            localRBBs.add(getUUID(conn));
        }
    }

    /**
     * Call this to determine if the server for this connection is in
     * the same address space as the client.
     *
     * Note there is intentionally no SQL binding for this - it is only called on the client side.
     */
    public static boolean isLocal(Connection conn) throws SQLException {
        conn.createStatement().executeQuery("call RBB_PRIVATE_SET_LOCAL()");
        synchronized(localRBBs) {
            return localRBBs.contains(getUUID(conn));
        }
    }

    public static double negative(double x) {
        // You might think RBB_NEGATIVE is unnecessary - why not just use -x?
        // That did work until h2 version 1.3.148, but not 1.3.162, where the special
        // case of -0.0 causes it to not work correctly.
        // See "incorrect result with negative zero (Simple test case)" in the
        // H2 Database google group on 22Dec2011

        if(x == 0.0)
            return 0.0;
        else
            return -x;
    }


    /**
     * The basic reason for this function is that H2 cannot pass a Long[]
     * into a stored procedure.
     */
    public static Long[] makeLongs(Object[] a) {
        Long[] result = new Long[a.length];
        for(int i = 0; i < a.length; ++i)
            if(a[i] instanceof Long)
                result[i] = (Long) a[i];
            else
                result[i] = Long.parseLong(a[i].toString());
        return result;
    }
}
