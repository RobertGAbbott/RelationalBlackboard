package gov.sandia.rbb.impl.h2;

import gov.sandia.rbb.Event;
import gov.sandia.rbb.RBB;
import gov.sandia.rbb.Tagset;
import static gov.sandia.rbb.Tagset.TC;
import java.net.ServerSocket;
import org.junit.Test;

/**
 *
 * @author rgabbot
 */
public class H2EventTCPServerTest {

    @Test
    public void testEventClientServer() throws Exception {
        final String methodName = java.lang.Thread.currentThread().getStackTrace()[1].getMethodName();
        System.err.println("Entering "+methodName);

        // claim port 1975 so rbb2 won't be able to!
        // This simulates the situation that some other process is using one or more ports.
        ServerSocket serverSocket = new ServerSocket(1975);

        RBB rbb = RBB.create("jdbc:h2:mem:"+methodName, "rbb");
        RBB rbb2 = RBB.create("jdbc:h2:mem:"+methodName+"2", "rbb2");
        RBB rbb3 = RBB.connect("jdbc:h2:mem:"+methodName); // same jdbc URL as first rbb

        EventCounter eventCounter = new EventCounter(rbb, true, new Tagset());
        EventCounter eventCounter2 = new EventCounter(rbb2, true, new Tagset());
        EventCounter eventCounter3 = new EventCounter(rbb3, true, new Tagset());

        new Event(rbb.db(), 0.0, 1.0, TC("x=y"));

        Thread.sleep(1000); // this tcp-based notification is asynchronous!  500ms was too short on a slow VM.

        eventCounter.assertCMDA(1, 0, 0, 0);
        eventCounter2.assertCMDA(0, 0, 0, 0);
        eventCounter3.assertCMDA(1, 0, 0, 0);


        rbb.disconnect();
        rbb2.disconnect();
        rbb3.disconnect();
    }

}