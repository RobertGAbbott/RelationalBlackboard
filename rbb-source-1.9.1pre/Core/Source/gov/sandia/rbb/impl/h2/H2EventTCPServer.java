
package gov.sandia.rbb.impl.h2;

import gov.sandia.rbb.RBB;
import gov.sandia.rbb.RBBEventChange;
import gov.sandia.rbb.RBBEventListener;
import gov.sandia.rbb.RBBFilter;
import gov.sandia.rbb.impl.h2.statics.H2SRBB;
import gov.sandia.rbb.util.StringsWriter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class ClientConnection extends Thread
{

    Socket _socket;

    String _RBB_UUID;

    // read the taglist from the socket.
    // we only send the taglist to the client before using the id for the first time, so we need to keep track of which sequences have already been identified.
    HashSet<Long> _seq_ids;

    java.util.concurrent.LinkedBlockingQueue<RBBEventChange> _q;

    public ClientConnection(String RBB_UUID, Socket socket)
    {
        super("H2EventTCPServer.ClientConnection"); // set a Thread name to aid in debugging
        _RBB_UUID = RBB_UUID;
        _q = new java.util.concurrent.LinkedBlockingQueue<RBBEventChange>();
        _socket = socket;
        _seq_ids = new HashSet<Long>();
    }

    private void startWaitingForClientToClose() {
        // Start a thread which does nothing but wait to read a -1 byte which indicates the client has exited.
        // It does seem hugely wasteful to use two threads (r+w) for each client, but there
        // is no alternative but to switch over to NIO (nonblocking I/O) (which would be a good idea sometime or other)
        new Thread() {
            @Override public void run() {
                Thread.currentThread().setName("H2EventTCPServer ClientConnection Reader"); // set a Thread name to aid in debugging
                try {
                    int read = _socket.getInputStream().read();
                    // System.err.println("Server got read() from client: " + read);
                }
                catch (IOException ex) {
                    // System.err.println("Server got exception reading from client.");
                }

                try {
                    _socket.close();
                }
                catch (IOException ex1) { }
            }
        }.start();
    }

    @Override
    public void run()
    {
        RBBEventListener listener = new RBBEventListener.Adapter () {
            @Override public void eventChanged(RBB rbb, RBBEventChange eventChange) {
                _q.offer(eventChange);
            }
        };
        
        // System.err.println("A client thread is running");
        try
        {
            final BufferedReader in = new BufferedReader(new InputStreamReader(_socket.getInputStream()));
            String tagline = in.readLine();
            if(tagline == null) {
                System.err.println("RBBEventListner - client disconnected before sending an initial request.  Listener exiting.");
                return;
            }
            startWaitingForClientToClose();
            Thread.currentThread().setName("H2EventTCPServer ClientConnection Writer tags="+tagline); // set a Thread name to aid in debugging
            RBBFilter filter = RBBFilter.fromString(tagline);
            H2EventTrigger.addListener(_RBB_UUID, listener, filter);
            StringsWriter sw = new StringsWriter();

            while(!_socket.isClosed()) // this detects if shutdown was initiated by the server side; the server closes the socket.  The socket cannnot be closed by the client, even if it disconnects cleanly.
            {
                RBBEventChange c = _q.poll(200, TimeUnit.MILLISECONDS); // come up for air once in a while to see if the socket has been closed.
                if(c == null)
                    continue;

                // write out the change as a string in the format expected by H2EventTCPClient
                sw.getBuffer().setLength(0); // erase previous contents
                c.toString(sw);
                sw.write("\n");
                // System.err.println("H2EventTCPServer Sending " + sw.toString());
                _socket.getOutputStream().write(sw.toString().getBytes());
             }
        }
        catch (Exception e)
        {
            System.err.println("Exception on clientconnection: " + e.toString());
        }

        H2EventTrigger.removeListener(_RBB_UUID, listener);
        // System.err.println("Client thread exiting " + Thread.currentThread().getId());
    }

}

public class H2EventTCPServer extends Thread
{
    /**
     * Map from the RBB UUID to the H2EventTCPServer currently running for it.
     */
    private static Map<String, H2EventTCPServer> _servers = new HashMap<String, H2EventTCPServer>();
    private static int _nextServerPort = 1974;

    private HashSet<ClientConnection> _clients;

    private int _port;

    private String _RBB_UUID;

    private ServerSocket _serverSocket;
    
    /**
     * call start() instead of using the constructor.
     */
    private H2EventTCPServer(Connection db) throws SQLException
    {
        super("H2EventTCPServer"); // set a Thread name to aid in debugging
        _RBB_UUID = H2SRBB.getUUID(db);
        _clients = new HashSet<ClientConnection>();

        // try multiple port numbers, keeping in mind that any could be already
        // claimed by another H2EventTCPServer in this process, in another process,
        // or claimed by a process unrelated to RBB.
        for(int i = 0; i < 100 && _serverSocket == null; ++i) {
            try {
                _port = _nextServerPort++;
                _serverSocket = new ServerSocket(_port);
            } catch(IOException e) {
                // This is OK, just try the next one...
                // System.err.println("IOException while trying to claim port " + _port + ": " + e.toString());
            }
        }

        if(_serverSocket == null)
            throw new SQLException("H2EventTCPServer failed to initialize; could not claim a server socket!");        
    }

    @Override
    public void run()
    {
        try
        {
            System.err.println("RBB TCP Event Server listening for connection on port " + _port);
            while (true)
            {
                Socket cs = _serverSocket.accept();
                // System.err.println("RBB TCP Event Server client accepted");
                ClientConnection cc = new ClientConnection(_RBB_UUID, cs);
                _clients.add(cc);
                cc.start();
            }
        }
        catch (Exception e)
        {
            if(!_serverSocket.isClosed()) // if it was explicitly closed, then the exception was expected.
                System.err.println("Server on port " + _port + " exiting on exception: " + e.toString());
        }
        finally {
            for(ClientConnection client : _clients) {
                try {
                    client._socket.close();
                }
                catch (IOException ex) {
                }
            }
            _clients.clear();
        }
        //System.err.println("H2EventTCPServer server socket exiting.");
    }

    /**
     * Stops the Event TCP Server (if one was running for this RBB in this process).
     *
     * If called directly, this function will only stop the server if this process happens
     * to be the server process.
     * 
     * If called through a SQL Query "RBB_STOP_EVENT_TCP_SERVER", or by calling H2SRBB.stopEventTCPServer,
     * then the TCP Server will be stopped in the server process, which will affect
     * any/all clients (not just ones in this process).
     *
     */
    public synchronized static void stop(Connection db) throws SQLException
    {
        final String uuid = H2SRBB.getUUID(db);
        H2EventTCPServer s = _servers.get(uuid);
        if(s != null) {
            try
            {
                s._serverSocket.close();
            }
            catch (IOException ex)
            {
                System.err.println("Error closing _serverSocket in H2EventTCPServer");
            }
            _servers.remove(uuid);
        }
    }

    /**
     *
     * Stop all the H2EventTCPServers running in this process,
     * even if there are several ones serving different RBBs.
     *
     * Note this will not stop H2EventTCPServers running in other processes.
     * 
     */
    public synchronized static void stopAllLocalServers() throws SQLException
    {
        for(H2EventTCPServer s : _servers.values()) {
            try
            {
                s._serverSocket.close();
            }
            catch (IOException ex)
            {
                System.err.println("Error closing _serverSocket in H2EventTCPServer");
            }
        }
        _servers.clear();
    }
    
    /**
     * Starts the Event TCP Server (if none was running for this RBB) and returns its port number.
     * To receive event updates, connect to the port (e.g. telnet localhost 1974) and send the tagset
     * of interest on one line.  Multiple tagsets can also be specified, delimited by the semicolon ;
     * <p>
     * This function should normally be called only through a SQL Query "RBB_START_EVENT_TCP_SERVER", or by calling H2SRBB.startEventTCPServer.
     * <p>
     * Calling this directly in a client process would start the TCP server in
     * the client process instead of the server process,
     * which doesn't work (since H2 triggers are called in the server process.)
     *
     */
    public synchronized static int start(Connection db) throws SQLException
    {
        final String uuid = H2SRBB.getUUID(db);
        H2EventTCPServer s = _servers.get(uuid);
        if(s == null) {
            if(_servers.isEmpty()) // issue warning message (only
                System.err.println("Starting RBB TCP server thread.  If this thread stops the process from exiting, add a call to H2EventTCPServer.stopAll().");

            s = new H2EventTCPServer(db);
            s.start();
            _servers.put(uuid, s);
        }

        return s._port;
    }


}
