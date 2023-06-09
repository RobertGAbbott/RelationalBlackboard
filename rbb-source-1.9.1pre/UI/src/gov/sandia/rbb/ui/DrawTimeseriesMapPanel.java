/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ui;

import gov.sandia.rbb.util.StringsWriter;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.Point;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import javax.imageio.ImageIO;
import org.openstreetmap.gui.jmapviewer.Coordinate;
import org.openstreetmap.gui.jmapviewer.JMapViewer;
import org.openstreetmap.gui.jmapviewer.MemoryTileCache;
import org.openstreetmap.gui.jmapviewer.OsmFileCacheTileLoader;
import org.openstreetmap.gui.jmapviewer.OsmMercator;
import org.openstreetmap.gui.jmapviewer.interfaces.MapRectangle;
import org.openstreetmap.gui.jmapviewer.interfaces.TileSource;
import org.openstreetmap.gui.jmapviewer.tilesources.BingAerialTileSource;
import org.openstreetmap.gui.jmapviewer.tilesources.OsmTileSource;

/**
 *
 * @author rgabbot
 */
public class DrawTimeseriesMapPanel extends JMapViewer implements DrawTimeseries.View {
    private DrawTimeseries drawTimeseries;

    public static enum MapType {
        Street(new OsmTileSource.Mapnik()),
        Topo(new OsmTileSource.CycleMap()),
        Photo(new BingAerialTileSource() {
            @Override public Image getAttributionImage() { // the implementation in JMapViewer jar has the incorrect path to this attribution image.
                try {
                    return ImageIO.read(getClass().getResourceAsStream("/org/openstreetmap/gui/jmapviewer/images/bing_maps.png"));
                } catch (IOException e) {
                    return null;
                }
            } }
        );

        TileSource tileSource;
        MapType(TileSource tileSource) {
            this.tileSource=tileSource;
        }
    }

    public DrawTimeseriesMapPanel(DrawTimeseries drawTimeseries, String mapType) {
        super(new MemoryTileCache() ,4); // must NOT call the default JMapView constructor because it creates a DefaultMapController that makes the map respond to mousewheel, right click, etc...
        this.drawTimeseries = drawTimeseries;
        try {
            File cacheDir = new File(System.getProperty("user.home"), ".OpenMapCache");
            this.setTileLoader(new OsmFileCacheTileLoader(this, cacheDir));
            System.err.println("Caching map tiles in "+cacheDir);
        } catch (Exception ex) {
            System.err.println("DrawTimeseriesMapPanel: Error setting tile loader");
        }

        try {
            TileSource src = MapType.valueOf(mapType).tileSource;
            setTileSource(src);
        } catch(IllegalArgumentException e) {
            throw new IllegalArgumentException("map must be one of: "+StringsWriter.join(" ", MapType.values()) + " (not "+mapType+"): "+e);
        }

        setZoomContolsVisible(false);
    }

    @Override
    public void paintComponent(Graphics g) {
        super.paintComponent(g); // draw the maps
        try {
            if(drawTimeseries.isDrawingPaths())
                drawTimeseries.drawPaths((Graphics2D)g);
//                drawTimeseries.drawPaths(g, null, null);
            drawTimeseries.draw(g);
        } catch (Exception ex) {
            System.err.println(ex);
        }

    }

    @Override
    public void zoom(Rectangle2D r) {
        setMapRectangleList(Arrays.asList(new MapRectangle[]{new Rect(r)}));
        setMapRectanglesVisible(false);
        setDisplayToFitMapRectangle();
    }

    @Override
    public Point2D toScreen(Point2D p) {
        return getMapPositionDouble(p.getX(), p.getY(), false);


    }

    @Override
    public Point2D fromScreen(Point2D q) {
        Coordinate p = getPosition((int)q.getX(), (int)q.getY());
        return new Point2D.Double(p.getLat(), p.getLon());
    }

    @Override
    public void panAndZoom(Point2D dataPoint, Point pixel, int zoomIncrement) {
        setDisplayPositionByLatLon(pixel, dataPoint.getX(), dataPoint.getY(), getZoom()+zoomIncrement);
    }


     /**
      * This is glue code - JMapViewier defines its own MapRectangle interface, here adapted to java.awt.geom.Rectangle2D
      */
     class Rect extends Rectangle2D.Double implements MapRectangle {
        Rect(Rectangle2D r) { super(r.getX(), r.getY(), r.getWidth(), r.getHeight()); }
        @Override public Coordinate getTopLeft() { return new Coordinate(getMinX(), getMinY()); }
        @Override public Coordinate getBottomRight() { return new Coordinate(getMaxX(), getMaxY()); }
        @Override public void paint(Graphics g, Point topLeft, Point bottomRight) { g.drawRect(topLeft.x, topLeft.y, bottomRight.x-topLeft.x, bottomRight.y-topLeft.y); }
     }

    /**
     *
     * This is adapted from JMapViewer because it had only integer precision, causing problems with heading estimation. - Rob
     *
     * Calculates the position on the map of a given coordinate
     *
     * @param lat
     * @param lon
     * @param checkOutside
     * @return point on the map or <code>null</code> if the point is not visible
     *         and checkOutside set to <code>true</code>
     */
    private Point2D getMapPositionDouble(double lat, double lon, boolean checkOutside) {
        double x = LonToX(lon, zoom);
        double y = LatToY(lat, zoom);
        x -= center.x - getWidth() / 2;
        y -= center.y - getHeight() / 2;
        if (checkOutside) {
            if (x < 0 || y < 0 || x > getWidth() || y > getHeight())
                return null;
        }
        return new Point2D.Double(x, y);
    }

     /**
     * This is adapted from JMapViewer because it had only integer precision, causing problems with heading estimation. - Rob
     * Transform longitude to pixelspace
     * @author Jan Peter Stotz
     */

    private static double LonToX(double aLongitude, int aZoomlevel) {
        int mp = OsmMercator.getMaxPixels(aZoomlevel);
        double x = ((mp * (aLongitude + 180l)) / 360l);
        x = Math.min(x, mp - 1);
        return x;
    }

    /**
     * This is adapted from JMapViewer because it had only integer precision, causing problems with heading estimation. - Rob
     *
     * Transforms latitude to pixelspace
     *
     * @author Jan Peter Stotz
     */
    private static double LatToY(double aLat, int aZoomlevel) {
        if (aLat < OsmMercator.MIN_LAT)
            aLat = OsmMercator.MIN_LAT;
        else if (aLat > OsmMercator.MAX_LAT)
            aLat = OsmMercator.MAX_LAT;
        double sinLat = Math.sin(Math.toRadians(aLat));
        double log = Math.log((1.0 + sinLat) / (1.0 - sinLat));
        int mp = OsmMercator.getMaxPixels(aZoomlevel);
        double y = (mp * (0.5 - (log / (4.0 * Math.PI))));
        y = Math.min(y, mp - 1);
        return y;
    }
}
