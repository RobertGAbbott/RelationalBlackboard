/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.impl.h2;
import gov.sandia.rbb.*;
import gov.sandia.rbb.RBBEventChange;
import gov.sandia.rbb.impl.h2.statics.H2SRBB;
import java.net.*;
import java.io.*;
import java.sql.SQLException;

/**
 * Together, H2EventTCPServer and H2EventTCPClient allow an EventListener
 * to be in a remote process from the DB server.
 *
 * This class connects to the socket provided by H2EventTCPServer and converts
 * socket messages into calls on an EventListener.
 *
 * @author rgabbot
 */
public class H2EventTCPClient extends Thread
{
    private RBB rbb;
    public RBBEventListener listener;
    RBBFilter filter;
    Socket socket;

    public H2EventTCPClient(
        RBB rbb,
        RBBEventListener listener,
        RBBFilter filter) throws SQLException
    {
        this.rbb = rbb;
        this.listener = listener;
        this.filter = filter;

        final int port = H2SRBB.startEventTCPServer(rbb.db());
        final String hostname = H2SRBB.getServerAddress(rbb.db());
        try
        {
            this.socket = new Socket(hostname, port);
        }
        catch (UnknownHostException ex)
        {
            throw new SQLException("H2EventTCPClient constructor got UnknownHostException: " + ex.toString());
        }
        catch (IOException ex)
        {
            throw new SQLException("H2EventTCPClient constructor got IOException: " + ex.toString());
        }

        // System.err.println("H2EventTCPClient established connection.");
    }

    public synchronized void close() throws IOException
    {
        // System.err.println("H2EventTCPClient closing socket.");
        listener = null;
        socket.close();
    }

    private static boolean warnOnce = true;

    @Override
    public void run()
    {
        if(warnOnce) {
            warnOnce=false;
            System.err.println("Starting an RBB H2EventTCPClient thread for "+filter+".  If the process is not exiting as expected make sure RBB.removeEventListener is called once for each RBB.addEventListener.");
        }

        try
        {
            PrintStream printStream = new PrintStream(socket.getOutputStream());

            // subscribe to events of interest
            String dataTagString = filter.toString();

            // Setting the name helps to identify this thread in the debugger.
            // If this thread is still alive and owns java.io.InputStreamReader and keeping your program alive,
            // you probably forgot to call RBB.removeEventListener after calling RBB.addEventListener.
            // Note these may be called indirectly by other classes (e.g. by RBBReplayControl) that have their own teardown functions.
            Thread.currentThread().setName("H2EventTCPClient: "+dataTagString);


            printStream.println(dataTagString);
            printStream.flush();
            // System.err.println("Subscribed for " + dataTagString);

            BufferedReader in = new BufferedReader(new InputStreamReader(
                socket.getInputStream()));

            while (true)
            {
                //  System.err.println("Waiting for event update");
                String line = in.readLine();
                if(line == null) // this happens when the server closes the connection.
                    break;
                // System.err.println(line);
                synchronized(this) { // close() could be called concurrently.
                    if(listener == null)
                        return;
                    else
                        dispatch(line);
                }
            }
        }
        catch (Exception e)
        {
            if(!socket.isClosed()) // don't complain if socket was explicitly closed.
              System.err.println("Exception in H2EventTCPClient.run: " + e.toString());
        }
    }

    void dispatch(String s) throws Exception
    {
        // System.err.println("H2EventTCPClient Receiving " + s);

        try
        {
            final String[] words = s.split("\t");
            final String cmd = words[0];
            final Event event = new Event(Long.parseLong(words[1]), Double.parseDouble(
                words[2]), Double.parseDouble(words[3]), new Tagset(words[4]));

            RBBEventChange change;

            if (cmd.equals("DataAdded"))
            {
                final String schemaName = words[5];
                final String tableName = words[6];
                Object[] data = new Object[words.length - 7];
                for (int i = 0; i < data.length; ++i) {
                    data[i] = words[i + 7];
                }
                change = new RBBEventChange.DataAdded(event, schemaName, tableName, data);
            }
            else if (cmd.equals("Added"))
            {
                change = new RBBEventChange.Added(event, Boolean.parseBoolean(words[5]));
            }
            else if (cmd.equals("Modified"))
            {
                change = new RBBEventChange.Modified(event);
            }
            else if (cmd.equals("Removed"))
            {
                change = new RBBEventChange.Removed(event, Boolean.parseBoolean(words[5]));
            }
            else
            {
                throw new Exception("RBBEventChange.fromString: invalid string " + s);
            }

            change.dispatch(rbb, listener);
        }
        catch (Exception ex)
        {
            System.err.println("H2EventTCPClient: ignoring exception the EventListener raised while processing line: \"" + s + "\": " + ex.toString());
        }

    }

}
