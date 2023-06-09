/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ui.chart;

import gov.sandia.rbb.Event;
import gov.sandia.rbb.RBB;
import gov.sandia.rbb.RBBEventChange;
import gov.sandia.rbb.RBBEventListener;
import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.Timeseries;
import gov.sandia.rbb.Timeseries.Sample;
import gov.sandia.rbb.TagsetComparator;
import gov.sandia.rbb.tools.RBBSelection;
import gov.sandia.rbb.ui.DrawTimeseries;
import gov.sandia.rbb.ui.RBBReplayControl;
import gov.sandia.rbb.util.StringsWriter;
import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import org.jfree.chart.ChartMouseEvent;
import org.jfree.chart.ChartMouseListener;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartRenderingInfo;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.entity.XYItemEntity;
import org.jfree.chart.labels.XYSeriesLabelGenerator;
import org.jfree.chart.plot.CombinedDomainXYPlot;
import org.jfree.chart.plot.ValueMarker;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYDataset;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RectangleEdge;
import org.jfree.ui.RefineryUtilities;
import static gov.sandia.rbb.RBBFilter.*;
 
/**
 * Plot RBB Timeseries against time.
 *
 * This is provided from the command-line as a utility, since it seems like a common use case.
 *
 * However, there are so many variations on how data can be drawn that many
 * applications will probably want to just use this as example code;
 * It would be a waste of time to re-package or encapsulate the full flexibility of JFreeChart.
 *
 */
public class TimeseriesXYChart extends JFreeChart {

    private RBB rbb, coordinationRBB;
    private Tagset filterTags;
    private ValueMarker timeMarker = null;
    private String[] dynamicFilterTags;
    private String[] splitOnFilterTags;
    private String[] nameTags;
    private String yLabel;
    private Double yMin, yMax;
    private Tagset timeCoordinate;
    private boolean drawShapes;
    private RBBSelection selections;
    private RBBReplayControl replayControl;

    TimeseriesXYChart(
            RBB rbb,
            RBB coordinationRBB,
            Tagset filterTags,
            boolean showCurrentTime,
            String yLabel,
            String[] dynamicFilterTags,
            String[] splitOnFilterTags,
            final String[] nameTags,
            Tagset timeCoordinate,
            Double yMin, Double yMax,
            boolean drawShapes
            ) throws SQLException {
        super(filterTags.toString(), // chart title,
              JFreeChart.DEFAULT_TITLE_FONT,
              new CombinedDomainXYPlot(),
              true); // do include a legend.)
        this.rbb = rbb;
        this.coordinationRBB = coordinationRBB;
        this.yLabel = yLabel;
        getLegend().setPosition(RectangleEdge.RIGHT);
        setBackgroundPaint(Color.white);

        // the combinedDomainXYPlot DOES use the x axis plot though.
        NumberAxis xAxis = new NumberAxis("Time");
        xAxis.setAutoRangeIncludesZero(false);
        getCombinedPlot().setDomainAxis(xAxis);

        this.filterTags = filterTags;

        this.dynamicFilterTags = dynamicFilterTags;
        this.splitOnFilterTags = splitOnFilterTags;
        this.nameTags = nameTags;

        startListeningForDataSelection();

        if(dynamicFilterTags != null)
            startListeningForFilterTags();

        if(showCurrentTime)
            constructTimeMarker();

        this.timeCoordinate = timeCoordinate;

        this.yMin = yMin;
        this.yMax = yMax;

        this.drawShapes = drawShapes;

        reconstructSubPlots();
    }

    private void constructTimeMarker() throws SQLException {
        timeMarker = new ValueMarker(0);
        timeMarker.setPaint(Color.BLACK);

        replayControl = new RBBReplayControl(coordinationRBB,
                new RBBReplayControl.Listener() {
           @Override public void replayControl(long simTime, double playRate) {
                timeMarker.setValue(simTime / 1000.0);
            }
        }, 100);
    }

    public final CombinedDomainXYPlot getCombinedPlot() {
        return (CombinedDomainXYPlot) super.getPlot();
    }

    private void startListeningForFilterTags() throws SQLException {
        // follow other applications when filterTags are changed.
        coordinationRBB.addEventListener(new RBBEventListener.Adapter() {
            @Override public void eventAdded(RBB rbb, RBBEventChange.Added ec) {
                try {
                    Tagset filterTagsSent = new Tagset(ec.event.getTagset().getValue("filterTags"));
                    for(String tagName : dynamicFilterTags)
                        filterTags.set(tagName, filterTagsSent.getValue(tagName));
                    reconstructSubPlots();
                } catch (SQLException ex) {
                    System.err.println(ex);
                }
            }
        }, byTags("filterTags"));
    }

    private void startListeningForDataSelection() throws SQLException {

        selections = new RBBSelection(rbb, coordinationRBB) {
            @Override
            public void selectionChanged(RBB rbb, Long RBBID, boolean selected) {
                int thickness = selected ? bold : notBold;
                    for(Object o : getCombinedPlot().getSubplots()) {
                        XYPlot plot = (XYPlot)o;
                        try {
                            int iSeries = plot.getDataset().indexOf(RBBID);
                            XYLineAndShapeRenderer render = (XYLineAndShapeRenderer) plot.getRenderer();
                            render.setSeriesStroke(iSeries, new BasicStroke(thickness));
                            render.setSeriesOutlineStroke(iSeries, new BasicStroke(thickness));
    //                        render.setDrawOutlines(thickness == bold);
                        } catch(Exception e) {
                            // dataset.indexOf will raise an exception quite often because the Event notifictaion
                            // may be for a timeseries we are not plotting.
                        }
                    }
            }

        };

        // start or stop drawing a timeseries in bold when it's selected or deselected.
        coordinationRBB.addEventListener(new RBBEventListener.Adapter() {
            @Override public void eventAdded(RBB rbb, RBBEventChange.Added ec) {
                setThickness(bold, ec.event);
            }
            @Override public void eventRemoved(RBB rbb, RBBEventChange.Removed ec) {
                setThickness(notBold, ec.event);
            }
            private void setThickness(int thickness, Event evt) {
                for(Object o : getCombinedPlot().getSubplots()) {
                    XYPlot plot = (XYPlot)o;
                    try {
                        Long RBBID = Long.parseLong(evt.getTagset().getValue("selected"));
                        int iSeries = plot.getDataset().indexOf(RBBID);
                        XYLineAndShapeRenderer render = (XYLineAndShapeRenderer) plot.getRenderer();
                        render.setSeriesStroke(iSeries, new BasicStroke(thickness));
                        render.setSeriesOutlineStroke(iSeries, new BasicStroke(thickness));
//                        render.setDrawOutlines(thickness == bold);
                    } catch(Exception e) {
                        // dataset.indexOf will raise an exception quite often because the Event notifictaion
                        // may be for a timeseries we are not plotting.
                    }
                }
            }
        }, byTags("selected"));
    }

    /**
     * This is overridden simply to add the 'synchronized' keyword.
     *
     * When the plots must be reconstructed due to a change to the RBB,
     * that occurs in a thread just for that purpose.  The changes trigger
     * drawing which is done in the UI thread.  Bad things happen if the UI
     * thread isn't forced to wait until reconstructSubPlots is finished.
     */
    public synchronized void draw(Graphics2D g2,
                     Rectangle2D chartArea, Point2D anchor,
                     ChartRenderingInfo info) {
        super.draw(g2, chartArea, anchor, info);
    }

    private void removeAllSubPlots() {
        ArrayList<XYPlot> subPlots = new ArrayList<XYPlot>();
        for(Object o : getCombinedPlot().getSubplots())
            subPlots.add((XYPlot)o);

        for(XYPlot subPlot : subPlots) {
            XYDataset dataset = subPlot.getDataset();
            try {
                ((TimeseriesXYDataset) dataset).destroy();
                getCombinedPlot().remove(subPlot);
            } catch (SQLException ex) {
            }
        }
    }

    private synchronized void reconstructSubPlots() throws SQLException {
        // System.err.println("Reconstruct subPlots with filterTags: " + filterTags);

        removeAllSubPlots();

        this.setTitle(filterTags.toString());

        if(splitOnFilterTags == null) {
            addSubPlot(filterTags, yLabel);
            return;
        }

        Tagset tags = new Tagset(filterTags);
        for(String tagName : splitOnFilterTags)
            tags.set(tagName, null);

        Tagset[] subPlotTags = Event.findTagCombinations(rbb.db(), tags.toString());
        TagsetComparator compare = new TagsetComparator();
        compare.compareNumbersAsNumbers(subPlotTags);
        Arrays.sort(subPlotTags, compare);
        for(Tagset t : subPlotTags) {
            Tagset yLabel0 = new Tagset();
            if(yLabel != null)
                yLabel0.add(yLabel,null);
            for(String tagName : splitOnFilterTags)
                yLabel0.add(tagName, t.getValue(tagName));
            addSubPlot(t, yLabel0.toString());
        }
    }

    private final int bold = 5; // thickness of line for selected timeseries
    private final int notBold = 1 ;

    private void addSubPlot(Tagset filterTags, String yLabel) throws SQLException {
        // if the filterTagsTemplate has undefined values, then initially just use the first matching values from the RBB.
        if(filterTags.containsValue(null)) {
            Event[] inRBB = Event.find(rbb.db(), byTags(filterTags));
            if(inRBB.length > 0) {
                filterTags = Tagset.template(filterTags, inRBB[0].getTagset());
            }
        }

        final TimeseriesXYDataset dataset = new TimeseriesXYDataset(rbb, byTags(filterTags), withTimeCoordinate(timeCoordinate));

        final XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
        renderer.setBaseShapesVisible(drawShapes);
        renderer.setBaseShapesFilled(false);
        if(nameTags != null) {
            final String currentYLabel = yLabel;
            renderer.setLegendItemLabelGenerator(new XYSeriesLabelGenerator() {
                @Override
                public String generateLabel(XYDataset dataset, int series) {
                    try {
                        Tagset tags = ((TimeseriesXYDataset) dataset).ts.findEvents()[series].getTagset();
                        ArrayList<String> nameValues = new ArrayList<String>();
//                        if(currentYLabel != null)
//                            nameValues.add(currentYLabel);
                        for(String name : nameTags)
                            nameValues.add(tags.getValue(name));
                        return StringsWriter.join(",", nameValues.toArray(new String[0]));
                    } catch (SQLException ex) {
                        return "Error";
                    }
                }
            });
         }

        NumberAxis yAxis = new NumberAxis(yLabel);
        yAxis.setAutoRangeIncludesZero(false);
        if(yMin != null && yMax != null)
           yAxis.setRange(yMin, yMax);
        final XYPlot plot = new XYPlot(dataset, null, yAxis, renderer);
        plot.setBackgroundPaint(Color.white);
        plot.setDomainGridlinePaint(Color.gray);
        plot.setRangeGridlinePaint(Color.gray);

        if(timeMarker != null)
            plot.addDomainMarker(timeMarker);

        // set initially selected set of timeseries as bold.
        for(Long RBBID : selections.getSelectedEventIDs()) try {
            int iSeries = dataset.indexOf(RBBID);
            renderer.setSeriesStroke(iSeries, new BasicStroke(bold));
        } catch(Exception e) {
        }

        getCombinedPlot().add(plot);
    }

    /**
     * This is for calling ChartMain from the command line - from java code call ChartMain directly instead.
     * If there is an exception, this main prints out the message and aborts the program with an error code.
     */
    public static void main(String[] args) {
        try
        {
            ChartMain(args);
        }
        catch(Throwable e)
        {
            System.err.println(e);
            System.exit(-1);
        }
    }

    /**
     * 
     * Creates a top-level window containing a chart for the specified data.
     *
     * unlike main(), ChartMain() propagates exceptions so it is more useful for
     * calling from other code or unit tests.
     *
     * To call it from other code you will typically:
     * import static gov.sandia.rbb.ui.TimeseriesChart;
     *
     */
    public static void ChartMain(String[] args) throws Throwable {

        StringWriter usage = new StringWriter();
        usage.write("chart [option...] RBBURL\n");
        usage.write("\t-coordinationRBB <RBBURL>: create or open the specified RBB for the current time (e.g. "+DrawTimeseries.exampleCoordinationRBB+").\n\t\tThis is implied if the main RBB is read-only, but can still be used to specify a different coordinationRBB.\n");
        usage.write("\t-drawCurrentTime: indicate the current time with a vertical bar.  (Assumes the primary RBB is used for time synchronization).\n");
        usage.write("\t-filterTags <tagset>: only plot timeseries with at least these tags.\n");
        usage.write("\t-dynamicFilterTags <tagname1,tagname2>: In addition to filterTags, plot only timeseries with these tag names, and the values for these tags taken from the most recent Event with a tag named filterTags.\n");
        usage.write("\t-noSymbols: don't draw a symbol for each sample of each timeseries.\n");
        usage.write("\t-splitOnFilterTags <tagname1,tagname2,...>: In addition to filterTags, plot only timeseries with these tag names, creating a separate plot for each combination of values of these tags.\n");
        usage.write("\t-selectionTemplate <tagname1=value1,tagname2,...>: when a Timeseries is clicked, get its values for these tag names, then select all timeseries matching the completed tagset.\n");
        usage.write("\t-timeCoordinate <tagset>: Use specified time coordinate when retrieving timeseries.\n");
        usage.write("\t-nameTag tagName1[,tagName2...]: Specify one or more tags whose values will be used as the timeseries labels.  Otherwise the Timeseries RBBID will be shown.\n");
        usage.write("\t-yRange <min> <max>: Specify the range of values to be visible in the y axis.\n");

        boolean drawCurrentTime = false;
        String rbbURL=null, coordinationRBBURL=null, yLabel=null;
        String[] dynamicFilterTags=null;
        String[] splitOnFilterTags=null;
        final Tagset selectionTemplate=new Tagset();
        String[] nameTags=null;
        Tagset filterTags=null;
        Tagset timeCoordinate = null;
        Double yMin=null, yMax=null;
        boolean drawShapes = true;
        
        if(args.length == 0)
            throw new Exception(usage.toString());

        int iArgs = 0;
        for(; iArgs < args.length && args[iArgs].substring(0,1).equals("-"); ++iArgs) {
            if(args[iArgs].equalsIgnoreCase("-drawCurrentTime"))
            {
                drawCurrentTime = true;
            }
            else if(args[iArgs].equalsIgnoreCase("-noSymbols"))
            {
                drawShapes = false;
            }
            else if(args[iArgs].equalsIgnoreCase("-coordinationRBB"))
            {
               if(++iArgs >= args.length)
                    throw new Exception("-coordinationRBB requires a JDBC URL.");
               coordinationRBBURL = args[iArgs];
            }
            else if(args[iArgs].equalsIgnoreCase("-filterTags"))
            {
               if(++iArgs >= args.length)
                    throw new Exception("-filterTags requires a tagset.");
               filterTags = new Tagset(args[iArgs]);
            }
            else if(args[iArgs].equalsIgnoreCase("-timeCoordinate"))
            {
               if(++iArgs >= args.length)
                    throw new Exception("-timeCoordinate requires a tagset.");
               timeCoordinate = new Tagset(args[iArgs]);
            }
            else if(args[iArgs].equalsIgnoreCase("-nameTag"))
            {
               if(++iArgs >= args.length)
                    throw new Exception("-nameTag requires a comma-separated list of tag names.");
               nameTags = args[iArgs].split(",");
            }
            else if(args[iArgs].equalsIgnoreCase("-dynamicFilterTags"))
            {
               if(++iArgs >= args.length)
                    throw new Exception("-dynamicFilterTags requires a comma-separated list of tag names.");
               dynamicFilterTags = args[iArgs].split(",");
            }
            else if(args[iArgs].equalsIgnoreCase("-splitOnFilterTags"))
            {
               if(++iArgs >= args.length)
                    throw new Exception("-splitOnFilterTags requires a comma-separated list of tag names.");
               splitOnFilterTags = args[iArgs].split(",");
            }
            else if(args[iArgs].equalsIgnoreCase("-selectionTemplate"))
            {
               if(++iArgs >= args.length)
                    throw new Exception("-selectionTemplate requires a comma-separated list of tag names.");
               selectionTemplate.add(new Tagset(args[iArgs])); // this is really just an assignment but selectionTemplate is final so the ChartMouseListener class below can use it from within.
            }
            else if(args[iArgs].equalsIgnoreCase("-yRange"))
            {
               if(++iArgs >= args.length-1)
                    throw new Exception("-yRange requires two numeric values.");
               yMin = Double.parseDouble(args[iArgs]);
               ++iArgs;
               yMax = Double.parseDouble(args[iArgs]);
            }
            else if(args[iArgs].equalsIgnoreCase("-yLabel"))
            {
               if(++iArgs >= args.length)
                    throw new Exception("-yLabel requires an argument");
               yLabel = args[iArgs];
            }
            else {
                throw new Exception("Unrecognized arg " + args[iArgs]);
            }

        }

        if(iArgs != args.length-1)
            throw new Exception("Error: <jdbcURL> required after the options");

        rbbURL = args[iArgs++];
        final RBB rbb = RBB.connect(rbbURL);

        final RBB coordinationRBB;
        if(coordinationRBBURL == null)
            coordinationRBBURL = rbbURL;
        if(coordinationRBBURL.equals(rbbURL))
            coordinationRBB = rbb;
        else
            coordinationRBB = RBB.connect(coordinationRBBURL);

        ApplicationFrame appFrame = new ApplicationFrame(yLabel != null ? yLabel : "Plot");

         final TimeseriesXYChart chart = new TimeseriesXYChart(
                rbb,
                coordinationRBB,
                filterTags,
                drawCurrentTime,
                yLabel,
                dynamicFilterTags,
                splitOnFilterTags,
                nameTags,
                timeCoordinate,
                yMin, yMax,
                drawShapes);

       ChartPanel chartPanel = new ChartPanel(chart);

        chartPanel.setPreferredSize(new java.awt.Dimension(800, 600));

        appFrame.add(chartPanel);

        //////// allow selection of timeseries by clicking on a data symbol on the plot.
        chartPanel.addChartMouseListener(new ChartMouseListener(){
            @Override
            public void chartMouseClicked(ChartMouseEvent e){
                try {
                    if (!(e.getEntity() instanceof XYItemEntity)) {
                        chart.selections.deselectAll();
                    }
                    else {
                        XYItemEntity item = (XYItemEntity) e.getEntity();
                        Timeseries ts = ((TimeseriesXYDataset) item.getDataset()).ts.findTimeseries()[item.getSeriesIndex()];
                        // System.err.println("Clicked timeseries: "+ts);
                        Sample sample = ts.getSample(item.getItem());
                        System.err.println("Clicked " + sample+" in " + ts);
                        Tagset selectTagset = null;
                        if(selectionTemplate.getNumTags() > 0) {
                            selectTagset = Tagset.template(selectionTemplate, ts.getTagset());
                            if(selectTagset.containsValue(null)) {
                                System.err.println("Warning: the user selected a Timeseries that didn't provide all the tags for the -selectionTemplate option.\n\tRequired:\t"+selectionTemplate+"\n\tSelected:\t"+ts.getTagset());
                                selectTagset = null; // fall back and select just the specific timeseries.
                            }
                        }
                        final Double selectionTime = chart.replayControl.getSimTime() / 1000.0; // todo: get current sim time.

                        if(selectTagset != null) // if selecting by tags, clicking on one Timeseries can result in the selection of multiple related Events.
                            chart.selections.selectEvents(byTags(selectTagset));
                        else
                            chart.selections.selectEvents(byID(ts.getID()));
                    }
                    } catch (SQLException ex) {
                        System.err.println(ex.toString());
                    }
            }
            @Override
            public void chartMouseMoved(ChartMouseEvent e){
            }
        });

        appFrame.pack();
        RefineryUtilities.centerFrameOnScreen(appFrame);
        appFrame.setVisible(true);
    }
 
};

