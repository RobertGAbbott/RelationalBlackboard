package gov.sandia.rbb.tools;

import gov.sandia.rbb.Event;
import gov.sandia.rbb.RBB;
import gov.sandia.rbb.RBBFilter;
import static gov.sandia.rbb.RBBFilter.*;
import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.impl.h2.statics.H2SBlob;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import javax.imageio.ImageIO;

/**
 * ImageEvent is a utility class for attaching images to RBB Events.
 *
 * They are stored as BLOBs in the images.images table.
 *
 * You can either work with ImageEvent instances,
 * or just use the static methods as conveniences to attach an image to any RBB Event.
 *
 * @author rgabbot
 */
public class ImageEvent extends Event {

    private BufferedImage image;

    public static final String schema = "images";

    public static ImageEvent createEvent(RBB rbb, double startTime, double endTime, Tagset tags, BufferedImage image, String formatName) throws java.sql.SQLException {
        Event event = new Event(rbb.db(), startTime, endTime, tags);
        attachImage(rbb, event.getID(), image, formatName);
        return new ImageEvent(event, image);
    }

    /**
     * The given eventID must identify an already-existing RBB Event with an associated image.
     */
    private ImageEvent(Event event, BufferedImage image) throws SQLException {
        super(event);
        this.image=image;
    }

    public BufferedImage getImage() {
        return image;
    }

    public static ImageEvent[] find(RBB rbb, RBBFilter... f)
        throws SQLException
    {
        RBBFilter filter = new RBBFilter(f);
        Event[] events = Event.find(rbb.db(), filter, bySchema(schema));
        ImageEvent[] imgEvents = new ImageEvent[events.length];

        for(int i = 0; i < events.length; ++i)
            imgEvents[i] = new ImageEvent(events[i], getImage(rbb, events[i].getID()));

        return imgEvents;
    }

    /**
     * Attach an image to an existing event.
     *
     * The "format" specifies the format in which the image will be saved -
     * "png", "jpg", or (256-color palletized) "gif"
     *
     */
    public static void attachImage(RBB rbb, long eventID, BufferedImage image, String formatName) throws SQLException {
        try
        {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            ImageIO.write(image, formatName, os);
            H2SBlob.attachBlob(rbb.db(), eventID, schema, new ByteArrayInputStream(os.toByteArray()));
        }
        catch (IOException ex)
        {
            throw new SQLException("ImageEvent.attachImage: " + ex.toString());
        }
    }
    /**
     * Get the image previously attached to an RBB Event
     */
    public static BufferedImage getImage(RBB rbb, long eventID) throws SQLException {
        try
        {
            return ImageIO.read(H2SBlob.getBlob(rbb.db(), eventID, schema));
        }
        catch (IOException ex)
        {
            throw new SQLException("H2SBlob.get Image: " + ex.toString());
        }
    }
}
