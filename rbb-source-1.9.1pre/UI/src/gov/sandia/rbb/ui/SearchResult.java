/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.sandia.rbb.ui;

import gov.sandia.rbb.Event;
import gov.sandia.rbb.RBB;
import java.awt.image.BufferedImage;
import java.sql.SQLException;

public class SearchResult {
    private RBB rbb;
    private Event event;
    private BufferedImage image;
    private String url;

    public SearchResult(RBB rbb, Event e, BufferedImage i, String url) {
        this.rbb = rbb;
        event = e;
        image = i;
    }
    public RBB getRBB() {
        return rbb;
    }

    public String getRbbName() {
        try {
        return rbb.getName();
        } catch(SQLException e)
        {
            return "(error)";
        }
    }
    public Event getEvent() {
        return event;
    }
    public BufferedImage getImage() {
        return image;
    }

    public String getRbbUrl() {
        return url;
    }
}
