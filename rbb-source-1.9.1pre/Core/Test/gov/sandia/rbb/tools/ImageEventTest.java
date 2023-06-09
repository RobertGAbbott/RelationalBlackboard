/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.tools;

import gov.sandia.rbb.Event;
import gov.sandia.rbb.Tagset;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.awt.Graphics;
import java.awt.Panel;
import javax.swing.JFrame;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import gov.sandia.rbb.RBB;
import org.junit.Test;
import static org.junit.Assert.*;
import static gov.sandia.rbb.RBBFilter.*;

/**
 *
 * @author rgabbot
 */
public class ImageEventTest {

    @Test
    public void testImageEvent() throws SQLException {
        System.err.println(
            "Entering "
            + java.lang.Thread.currentThread().getStackTrace()[1].getMethodName());
        final String dbURL = "jdbc:h2:mem:Test";
        RBB rbb = RBB.create(dbURL, null);

        // make a test image
        BufferedImage img =
            new BufferedImage(100, 100,
            BufferedImage.TYPE_3BYTE_BGR);
        Graphics2D g = img.createGraphics();
        g.drawString("png", 10, 15);

        // create an ImageEvent.
        ImageEvent event =
            ImageEvent.createEvent(rbb, 1.0, 2.0,
            new Tagset("name=image1"), img, "png");

        // create some other irrelevant event, just to ensure it is not found by ImageEvent.find()
        new Event(rbb.db(), 1.0, 10.0, new Tagset("hi=there"));

        // make sure we can find the event in the RBB
        final ImageEvent[] events = ImageEvent.find(rbb);
        assertEquals(1, events.length);
        rbb.disconnect();
    }


    public static void main(String[] args)
    {
        try
        {
            System.err.println("Entering " +
                java.lang.Thread.currentThread().getStackTrace()[1].getMethodName());
            final String dbURL = "jdbc:h2:mem:Test";
            RBB rbb = RBB.create(dbURL, null);
 
            // make a test image
            BufferedImage img =
                new BufferedImage(100, 100,
                BufferedImage.TYPE_3BYTE_BGR);
            Graphics2D g = img.createGraphics();
            g.drawString("png", 10, 15);

            // create an ImageEvent.
            ImageEvent event =
                ImageEvent.createEvent(rbb, 1.0, 2.0,
                new Tagset("name=image1"), img, "png");

            // create some other irrelevant event, just to ensure it is not found by ImageEvent.find()
            new Event(rbb.db(), 1.0, 10.0, new Tagset("hi=there"));

            // make sure we can find the event in the RBB
            final ImageEvent[] events = ImageEvent.find(rbb, null, null, null, null);
            assertEquals(1, events.length);
            rbb.disconnect();

            // create a test window to show the test image.
            final JFrame frame = new JFrame();
            frame.getContentPane().add(new Panel() {
                public void paint(Graphics g) {
                    g.drawImage(events[0].getImage(), 0, 0, null);
                }
            });

            frame.setSize(100, 100);
            frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            frame.setVisible(true);
        }
        catch (SQLException ex)
        {
            Logger.getLogger(ImageEventTest.class.getName()).log(Level.SEVERE,
                null, ex);
        }
    }

}