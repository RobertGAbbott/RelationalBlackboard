
package gov.sandia.rbb.ui;

import javax.swing.JComponent;
import java.util.Comparator;
import javax.swing.JMenuItem;
import javax.swing.JPopupMenu;
import gov.sandia.rbb.RBBEventChange.DataAdded;
import gov.sandia.rbb.tools.RBBValues;
import java.awt.BasicStroke;
import java.util.Collections;
import javax.swing.AbstractAction;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import gov.sandia.rbb.*;
import gov.sandia.rbb.EventCache;
import gov.sandia.rbb.RBBEventChange;
import gov.sandia.rbb.TagsetComparator;
import gov.sandia.rbb.Tagset;
import static gov.sandia.rbb.Tagset.TC;
import gov.sandia.rbb.Timeseries.Sample;
import gov.sandia.rbb.RBB;
import gov.sandia.rbb.impl.h2.statics.H2SEvent;
import gov.sandia.rbb.impl.h2.statics.H2STimeseries;
import java.awt.Color;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.AdjustmentEvent;
import java.awt.event.AdjustmentListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.awt.event.KeyEvent;
import java.awt.event.MouseEvent;
import java.awt.event.MouseWheelEvent;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollBar;
import javax.swing.JTextField;
import javax.swing.JToggleButton;
import javax.swing.JToolBar;
import javax.swing.Timer;
import gov.sandia.rbb.tools.ImageEvent;
import gov.sandia.rbb.tools.RBBSelection;
import gov.sandia.rbb.util.StringsWriter;
import java.awt.EventQueue;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.awt.event.KeyListener;
import java.awt.event.MouseListener;
import java.awt.event.MouseMotionListener;
import java.awt.event.MouseWheelListener;
import java.awt.geom.AffineTransform;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.imageio.ImageIO;
import javax.swing.ButtonGroup;
import javax.swing.ImageIcon;
import javax.swing.JComboBox;
import javax.swing.JOptionPane;
import static gov.sandia.rbb.RBBFilter.*;

/**
 *
 * Interactively draw timeseries data.
 * This is useful to view or create RBB timeseries.
 * A good example of customizing this application through command-line options can be found in gov.sandia.rbb.examples.jsaf.JSAFExample.
 *
 * @author rgabbot
 */

public class DrawTimeseries implements RBBReplayControl.Listener,
    MouseMotionListener, MouseListener, MouseWheelListener, KeyListener

{

    private Thread _notifyMe;

    public RBB rbb;

    EventCache tsCache;

    /**
     *
     * Following the nomeclature from RBB ML,
     * this is the RBB used for maintaining the current interaction state of the user with the RBB.
     * This in includes time synchronization, selection of timeseries, and snapshot images (snapshotSelectedEntities).
     *
     * If -coordinationRBB is not specified, then _coordinationRBB == rbb
     *
     */
    public RBB _coordinationRBB;

    /**
     *
     * Following the nomeclature from RBB ML,
     * this is the RBB used for retrieving 'predictions'- the output of an RBB ML model,
     * which in this case means flags so lines can be drawn to input events.
     *
     * If -predictionsRBB is not specified, then _predictionsRBB == rbb
     *
     **/
    private RBB _predictionsRBB;
    EventCache flagCache;

    /**
     * For implementing the selectionTemplate option.  See the usage string.
     */
    private Tagset selectionTemplate = null;

    private org.h2.tools.Server _h2server, _webserver;

    private Long _prevDrawTime;
    private boolean _outputFPS = false;

    private JTextField _timeStepText, _timeText;

    private JComboBox _create_with_tags, _filter_tags;
    
    /**
     * This is where the drawing occurs.
     * The panel *may* also want to change the data-to-screen coordinate mapping;
     * in that case it should also override View
     */
    private JPanel _panel;

    /**
     * If the JPanel implements the View interface then _view will reference it;
     * otherwise _view will implement a default, y-up coordinate transformation.
     */
    View _view;

    interface View {
        /**
         * Adjust the view to include at least the specified rectangle, in data (not screen) coordinates.
         */
        public void zoom(Rectangle2D r);

        /**
         * pan and zoom the view so the specified datapoint is shown at the specified
         * pixel location, and zoom is (approximately) doubled (1), halved (-1), or left the same (0)
         */
        public void panAndZoom(Point2D dataPoint, Point pixel, int zoomIncrement);

        /**
         * Project data onto the screen
         */
        Point2D toScreen(Point2D p);

        /**
         * Project pixel coordinate into data.
         */
        Point2D fromScreen(Point2D q);
    }

    RBBEventUI eventChangeUI;

    long _timeStepMs;

    /**
     * playRate is the replay rate ordered by this application when play is hit.
     * So, for example, _replayState.getPlayRate() is 0 unless this is currently playing,
     * whereas playRate still reflects the speed that *will* be ordered when Play is selected.
     * playRate is updated by the command of a non-zero replay rate by another ReplayController.
     *
     */
    Double _playRate = 1.0;

    ArrayList<String> _labels; // label each timeseries with the value(s) of these tags.

    /**
     * <pre>
     * The dragging or clicking the mouse can have different effects, depending on
     * which button was pressed, mode buttons (zoom button),...
     * The logic for deciding which is in MousePressed.
     *
     * _mouseAction reflects the mouse action as of the previous MousePressed,
     * and left that way until the next _mousePressed.
     * This is because mouseClicked is fired after mouseReleased, but only if 
     * there was no mouseDragged between mousePressed and mouseReleased.  So there
     * is no obvious time to clear the previouse _mouseAction.
     *
     * This implementation of having the tooltip text, icon name, and hotkey in an
     * enum is nonstandard - normally you would use an Action.  However the hotkeys
     * work differently in this case (they're metakeys, yet take action - selecting
     * the mouse mode - instantly, instead of modifying a subsequent keypress).
     * So, the Action model required one Action for pressing the key, another
     * for releasing it (which would call setSelected(false) on the first),
     * and registering them all with an ActionMap,
     * was tried but the code was quite a bit longer with no advantages.
     * </pre>
     */
    enum MouseMode { 
        ZOOM("Zoom Mode: Click to zoom out, or click and drag to zoom on an area (or use the mouswheel, regardless of mode)", "MarqueeZoomIcon.png", KeyEvent.VK_ALT),
        DRAW("Draw Mode: Click and drag to draw new timeseries.  The Play button must be selected.", "DrawIcon.png", null), // there's no hotkey for DRAW; it's the default.
        PAN("Pan Mode: Click and drag to pan the display (or right-click-and-drag, regardless of mode).", "PanIcon.png", KeyEvent.VK_META),
        SELECT("Selection Mode: Click and drag to group-select, or click on a timeseries to select it.  Click empty space to de-select all.", "SelectIcon.png", KeyEvent.VK_SHIFT),
        NONE;

        Integer keyCode;
        String iconName;
        String toolTipText;

        MouseMode() {};
        MouseMode(String toolTipText, String iconName, Integer keyCode) {
            this.toolTipText = toolTipText;
            this.iconName = iconName;
            this.keyCode = keyCode;
        }
        String getToolTipText() {
            if(keyCode != null)
                return KeyEvent.getKeyText(keyCode)+": "+toolTipText;
            else
                return toolTipText;
        }
    };

    MouseMode _mouseMode = MouseMode.NONE;
    Map<MouseMode, JToggleButton> _mouseModeButtons; // when a hotkey is pressed we need to find the corresponding JToggleButton to do setSelected.
    Point _mouse_pressed, _mouse_dragged; // mouse screen location as of previous mousePressed / mouseDragged event.
    Timeseries _mouseDrawingTimeseries; // timeseries currently being interactively drawn
    Point2D _mouse_pressed_fromscreen; // where (in data coordinates) the mouse was pressed.

    public JFrame _frame;

    JScrollBar _timeScroll;
    Double _minTime = 0.0, _maxTime =  60.0; // min/max times currently displayed.

    RBBReplayControl _replayControl;

    /**
     * If the user specifies a -background image it is stored here.
     */
    Image _backgroundImage;

    // TODO: history replaces 'dot'
    private Color _dotColor = Color.GRAY; // color in which to draw dots if no color=xxx tag is present.  If null, no dots are drawn.
    private Color _selectedColor = Color.YELLOW; // color for drawing selected data.
    RBBSelection selections;

    String _timeCoordinate;

    public boolean isDrawingPaths() { return _drawPaths.isSelected(); };
    public boolean isDrawingHistory() { return _drawHistory.isSelected(); };
    JToggleButton _drawPaths, _drawHistory, _playButton;

    RBBValues _historyLength; // the name of the group is "historyLength" and the values are "before" and "after"

    Rectangle2D _initialZoom = new Rectangle2D.Double(0,0,0,0);

    /**
     * map tagset to icon for timeseries
     **/
    class Icon
    {
        Icon(Tagset tags,
            BufferedImage img)
            throws SQLException {
            this.tags = tags;
            this.img = img;
        }

        Tagset tags;
        BufferedImage img;
    }
    ArrayList<Icon> icons;
    double _iconScale=1.0;

    class GatherTags {
        String[] joinTagNames;
        String[] addTagNames;
        String jdbc;
        EventCache eventCache;

        GatherTags(String jdbc, String[] joinTagNames, String[] addTagNames) throws SQLException {
            this.joinTagNames=joinTagNames;
            this.addTagNames=addTagNames;
            this.jdbc = jdbc;
        }
        Tagset gatherTags(Double time, Tagset startTags) throws SQLException {
            if(eventCache == null) {
                RBB rbb = RBB.connect(jdbc);
                eventCache=new EventCache(rbb);
                Tagset t = new Tagset();
                for(String j : joinTagNames)
                    t.add(j,null);
                for(String a : addTagNames)
                    t.add(a, null);
                eventCache.initCache(byTags(t));
                // trigger repaint when the events from which we are gathering tags changes,
                // since that will likely change how things are drawn.
                rbb.addEventListener(new RBBEventListener.Adapter() {
                    @Override public void eventChanged(RBB rbb, RBBEventChange ec) { repaint(true); }
                } , byTags(t));
            }
            
            Tagset queryTags = new Tagset();
            for(String j : joinTagNames) {
                final String v = startTags.getValue(j);
                if(v==null)
                    throw new SQLException("gatherTags error: tagset "+startTags+" does not provide a value for join tag "+j);
                queryTags.add(j, v);
            }
            for(String a : addTagNames)
                queryTags.add(a, null);
            Event[] matches = eventCache.findEvents(byTags(queryTags), byTime(time, time));
            if(matches.length==0)
                throw new SQLException("gatherTags found no solution");
            Tagset result = startTags.clone();
            for(String a : addTagNames)
                result.set(a, matches[0].getTagset().getValue(a));
            return result;
        }
    }

    ArrayList<GatherTags> gatherTags;

    private InputStream openResourceOrFile(String pathname) throws FileNotFoundException {
        java.io.InputStream is = getClass().getResourceAsStream(pathname);
        if(is == null)
            return new FileInputStream(pathname);
        return is;
    }

    private void dieWithMessage(String msg) {
        System.err.println(msg);
        System.exit(1);
    }

    public final static String exampleCoordinationRBB = "jdbc:h2:mem:coordination";


    /*
     * If not specified in constructor args,
     * is left as null, meaning whatever it was previously in the RBB.
     */
    Double initialTime = null;

    /**
     * Whether to show paths on startup.  Not used afterwards.
     */
    boolean showPathsInitially = false;

    String panelClass = null;

    /*
     * Type of map (if any) specified by the arguments - Street, Topo, Photo
     */
    String mapType = null;

    /**
     * Errors in parameters are detected and an exception thrown from the calling thread.
     * Then GUI initialization is scheduled to be executed by the Swing event thread.
     *
     * @param args
     * @throws Exception
     */
    public DrawTimeseries(String... args) throws Exception
    {
        StringWriter usage = new StringWriter();
        usage.write("draw [option...] <dbURL>\n");
        usage.write("\t-background <image>: set a background image.  'image' specifies a file or resource\n");
        usage.write("\t-coordinationRRB <RBBURL>: create or open the specified RBB for time synchronization  (e.g. "+exampleCoordinationRBB+").\n\t\tThis is implied if the main RBB is read-only, but can still be used to specify a different coordinationRBB.\n");
        usage.write("\t-create: create the RBB if it does not already exist.  If this is not specified, the dbURL must already be an RBB.\n");
        usage.write("\t-initialTime <123.456>: set the current time to the specified value at startup\n");
        usage.write("\t-dotColor <color>: set the default color for data dots (for Timeseries with no color=<color> tag).  Can be a code (eg 0xff8000) or any of:\n\t\tBLACK, BLUE, CYAN, DARK_GRAY, GRAY, GREEN, LIGHT_GRAY, MAGENTA, ORANGE, PINK, RED, WHITE, YELLOW\n");
        usage.write("\t-eventTags <tagName1=tagValue1[,tagName2=tagValue2...]>: add to list of tagset presets for drawing new timeseries.\n\t\tMultiple -eventTags options are allowed.\n");
        usage.write("\t-filterTags <tagName1=tagValue1[,tagName2=tagValue2...]>: add to the list of tagsets to use as a filter for which timeseries to draw.\n");
        usage.write("\t-filterTagsMulti <tagName1[=tagValue1][,tagName2[=tagValue2...]]>: add all combinations of matching tagsets a filters for which timeseries to draw.  Can be specified multiple times.\n");
        usage.write("\t-fps: compute frames per second drawn and output to stderr with each redraw.\n");
        usage.write("\t-gatherTags <dbURL> <joinTagName[,..]> <addTagName[,...]>: add tags to each timeseries before drawing it by finding an RBB event\n\t\tin the specified RBB that has the same values for the joinTagNames, and provides values for all the addTagNames.\n\t\tThis is applied after finding timeseries with the -filterTags option, but before selecting an icon or color.\n\t\tMay be specified multiple times.\n");
        usage.write("\t-selectionTemplate <templateTagset>: when a Timeseries is clicked, fill in null values from the template using the selected timeseries' tags, then select all timeseries matching the completed template.\n");
        usage.write("\t-help: display this help message and exit\n");
        usage.write("\t-icon <tagset> <image>: use the specified icon for events matching the tagset.  Multiple -icon args are allowed.\n\t\tThey are applied in order and only the first match is used.\n");
        usage.write("\t-label tagname: label each timeseries with the value of the specified tag.  Can be specified multiple times.\n");
        usage.write("\t-minTime <123.456>: set the time corresponding to the leftmost position of the time scroll bar\n");
        usage.write("\t-map <Street|Topo|Photo>: Draw a map background using OpenStreetMap.  Requires JMapViewer.jar.\n\t\tThis option interprets 2D timeseries as latitude/longitude in decimal degrees.\n");
        usage.write("\t-maxTime <123.456>: set the time corresponding to the rightmost position of the time scroll bar\n");
//        usage.write("\t-noCacheTimeseries: Conserve RAM by not caching the timeseries data to be drawn.\n");
        usage.write("\t-noDots: don't draw dots for timeseries (normally used with -icon)\n");
        usage.write("\t-predictionsRRB <RBBURL>: open the specified RBB for events with type=flag,xxx.RBBID=<eventID>.\n");
        usage.write("\t-server: host the H2 tcp and web servers so other clients (including remote clients) can connect.\n");
        usage.write("\t-showPaths: initially enable Show Paths.\n");
        usage.write("\t-snapshotSelected: periodically grab an image of selected data and attach to an RBB event 'type=dataSelection' in the 'images' schema.\n");
        usage.write("\t-speed <1.0>: units of time in the tool are 1/speed seconds\n");
        usage.write("\t-timeCoordinate <tagset>: Use specified time coordinate when retrieving timeseries.\n");
        usage.write("\t-timestep <0.025>: units of time between redraw\n");
        usage.write("\t-zoom <x1> <y1> <x2> <y2>: specify initial zoom settings.\n");
        usage.write("\tdbURL: a jdbc URL, e.g. jdbc:h2:file:///tmp/mydb.  If not specified, an in-memory database is created.");

        if(args.length == 0)
            dieWithMessage(usage.toString());

        // on the mac this causes the menu to appear in the menu area at the top of the screen.
        // Otherwise it will appear inside the application window.
        System.setProperty("apple.laf.useScreenMenuBar", "true");

        final DrawTimeseries ds_this = this;

        this.icons = new ArrayList<Icon>();
        this.gatherTags = new ArrayList<GatherTags>();
        
        _mouseDrawingTimeseries = null;
        _timeStepMs = 25; // milliseconds
        _filter_tags = new JComboBox();

        _create_with_tags = new JComboBox();
        
        ArrayList<String> filterTagsMulti = new ArrayList<String>();
        _labels = new ArrayList<String>();

        boolean createRBB = false;
        showPathsInitially = false;

        // The panel is where the drawing is shown.
        // By default it's just a JPanel.
        // This can be set to another class to show a different background, with these requirements:
        // 1) it must inherit from JPanel
        // 2) it must have a public 1-arg constructor that takes a DrawTimeseries instance
        // 3) paintComponent should almost certainly call DrawTimeseries.paintPanel after doing whatever specialized painting it does.
        // 4) if the panel's functionality depends on the center of view or zoom, then it should implement ZoomConstraint.
        // The initial purpose of this is to implement an OpenStreetMap background, with no dependency on JMapViewer.jar unless the map is actually used.
        

        int iArgs = 0;
        for(; iArgs < args.length && args[iArgs].substring(0,1).equals("-"); ++iArgs) {
            if(args[iArgs].equalsIgnoreCase("-background"))
            {
                ++iArgs;
                if(iArgs >= args.length)
                    dieWithMessage("-background requires an image pathname (or resource name).  Try running with -help.");
                this._backgroundImage = ImageIO.read(openResourceOrFile(args[iArgs]));
            }
            else if(args[iArgs].equalsIgnoreCase("-icon"))
            {
                if(iArgs+2 >= args.length)
                    dieWithMessage("-icon requires an image pathname (or resource name).  Try running with -help.");
                 this.icons.add(new Icon(new Tagset(args[iArgs+1]), ImageIO.read(openResourceOrFile(args[iArgs+2]))));
                iArgs += 2;
            }
            else if(args[iArgs].equalsIgnoreCase("-help"))
            {
                dieWithMessage(usage.toString());
            }
            else if(args[iArgs].equalsIgnoreCase("-minTime"))
            {
                if(++iArgs >= args.length)
                    dieWithMessage("-minTime requires a numeric argument.  Try running with -help.");
                _minTime = Double.parseDouble(args[iArgs]);
            }
            else if(args[iArgs].equalsIgnoreCase("-maxTime"))
            {
                if(++iArgs >= args.length)
                    dieWithMessage("-maxTime requires a numeric argument.  Try running with -help.");
                _maxTime = Double.parseDouble(args[iArgs]);
            }
            else if(args[iArgs].equalsIgnoreCase("-initialTime"))
            {
                if(++iArgs >= args.length)
                    dieWithMessage("-initialTime requires a numeric argument.  Try running with -help.");
                initialTime = Double.parseDouble(args[iArgs]);
            }
            else if(args[iArgs].equalsIgnoreCase("-timestep")) {
                if(++iArgs >= args.length)
                    dieWithMessage("-timestep requires a numeric argument specified in seconds.  Try running with -help.");
                _timeStepMs = 1000L*(long)(Double.parseDouble(args[iArgs]));
            }
            else if(args[iArgs].equalsIgnoreCase("-speed")) {
                if(++iArgs >= args.length)
                    dieWithMessage("-speed requires a numeric argument.  Try running with -help.");
                _playRate = Double.parseDouble(args[iArgs]);
            }
            else if(args[iArgs].equalsIgnoreCase("-filterTags"))
            {
               if(++iArgs >= args.length)
                    dieWithMessage("-filterTags requires a tagset (name1=value1,name2=value2,...).  Try running with -help.");
               _filter_tags.addItem(args[iArgs]);
            }
            else if(args[iArgs].equalsIgnoreCase("-filterTagsMulti"))
            {
               if(++iArgs >= args.length)
                    dieWithMessage("-filterTagsMulti requires a tagset (name1[=value1][,name2[=value2,...]]).  Try running with -help.");
               filterTagsMulti.add(args[iArgs]);
            }
            else if(args[iArgs].equalsIgnoreCase("-coordinationRBB")) {
               if(++iArgs >= args.length)
                    dieWithMessage("-coordinationRBB requires an RBB URL.  Try running with -help.");
                String jdbc = args[iArgs];
                try {
                    _coordinationRBB = RBB.connect(jdbc);
                } catch(SQLException e) {
                    if(e.getSQLState().equals("90013")) // means database was not found.
                        _coordinationRBB = RBB.create(jdbc, null);
                }
            }
            else if(args[iArgs].equalsIgnoreCase("-predictionsRBB")) {
               if(++iArgs >= args.length)
                    dieWithMessage("-predictionsRBB requires an RBB URL.  Try running with -help.");
                String jdbc = args[iArgs];
                _predictionsRBB = RBB.connect(jdbc);
            }
            else if(args[iArgs].equalsIgnoreCase("-label"))
            {
               if(++iArgs >= args.length)
                    dieWithMessage("-label requires a tag name.  Try running with -help.");
               _labels.add(args[iArgs]);
            }
            else if(args[iArgs].equalsIgnoreCase("-eventTags")) {
               if(++iArgs >= args.length)
                    dieWithMessage("-eventTags requires a tagset (name1=value1,name2=value2,...).  Try running with -help.");
               _create_with_tags.addItem(args[iArgs]);
            }
            else if(args[iArgs].equalsIgnoreCase("-gatherTags")) {
                if(iArgs+3 >= args.length)
                    dieWithMessage("-gatherTags requires 3 args: <dbURL> <joinTagName[,..]> <addTagName[,...]>");
                String jdbc = args[++iArgs];
                String[] joinTagNames = args[++iArgs].split(",");
                String[] addTagNames = args[++iArgs].split(",");
                this.gatherTags.add(new GatherTags(jdbc, joinTagNames, addTagNames));
            }
            else if(args[iArgs].equalsIgnoreCase("-selectionTemplate")) {
                if(iArgs+1 >= args.length)
                    dieWithMessage("-selectionTemplate requires a tagset, e.g. name1,name2,name3=value3");
                selectionTemplate = TC(args[++iArgs]);
            }
            else if(args[iArgs].equalsIgnoreCase("-zoom")) {
                if(iArgs+4 >= args.length)
                    dieWithMessage("-zoom requires 4 args: x1 y1 x2 y2");
                Double x1 = Double.parseDouble(args[++iArgs]);
                Double y1 = Double.parseDouble(args[++iArgs]);
                Double x2 = Double.parseDouble(args[++iArgs]);
                Double y2 = Double.parseDouble(args[++iArgs]);
                _initialZoom = new Rectangle2D.Double(x1, y1, 0, 0);
                _initialZoom.add(x2,y2);
            }
            else if(args[iArgs].equalsIgnoreCase("-fps")) {
                this._outputFPS = true;
            }
            else if(args[iArgs].equalsIgnoreCase("-showPaths")) {
                showPathsInitially = true;
            }
            else if(args[iArgs].equalsIgnoreCase("-map")) {
               if(++iArgs >= args.length)
                    dieWithMessage("-map requires a map type argument.  Try running with -help.");
                mapType = args[iArgs];
                panelClass = "gov.sandia.rbb.ui.DrawTimeseriesMapPanel";
            }
            else if(args[iArgs].equalsIgnoreCase("-noDots")) {
                this._dotColor = null;
            }
            else if(args[iArgs].equalsIgnoreCase("-dotColor")) {
               if(++iArgs >= args.length)
                    dieWithMessage("-dotColor requires a color.  Try running with -help.");
                this._dotColor = colorForString(args[iArgs], null);
                if(this._dotColor == null)
                    dieWithMessage("Invalid color specified for -dotColor: "+args[iArgs]+".  Run with -help to list color options.");
            }
            else if(args[iArgs].equalsIgnoreCase("-server")) {
                _h2server = org.h2.tools.Server.createTcpServer("-tcpAllowOthers");
                _h2server.start();

                _webserver = org.h2.tools.Server.createWebServer("-webAllowOthers");
                _webserver.start();
            }
            else if(args[iArgs].equalsIgnoreCase("-snapshotSelected")) {
                dataSelectionSnapshotTimer.start();
            }
            else if(args[iArgs].equalsIgnoreCase("-timeCoordinate")) {
               if(++iArgs >= args.length)
                    dieWithMessage("-timeCoordinate requires a tagset argument.  Try running with -help.");
                this._timeCoordinate = args[iArgs];
            }
            else if(args[iArgs].equalsIgnoreCase("-create")) {
                createRBB=true;
            }
            else {
                dieWithMessage("Unrecognized arg " + args[iArgs] + ".  Try running with -help.");
            }
        }

        if(iArgs >= args.length)
            dieWithMessage("Error: a JDBC_URL is required.  For a shared temporary RBB, try -server -create jdbc:h2:tcp:localhost/mem:test");

        String jdbc = args[iArgs];

        if(createRBB)
            rbb = RBB.createOrConnect(jdbc);
        else
            rbb = RBB.connect(jdbc);

        System.err.println("opened: " + args[iArgs]);


        if(_coordinationRBB == null) {
            if(rbb.db().isReadOnly()) {
                System.err.println("RBB is read-only; using default -coordinationRBB "+exampleCoordinationRBB);
                _coordinationRBB = RBB.createOrConnect(exampleCoordinationRBB);
            }
            else
                _coordinationRBB = rbb;
        }
        selections = new RBBSelection(rbb, _coordinationRBB) {
            @Override public void selectionChanged(RBB rbb, Long ID, boolean selected) {
                ds_this.repaint(true);
            }
        };

        if(_predictionsRBB == null)
            _predictionsRBB = rbb;

        if(_timeCoordinate != null && gatherTags.size() > 0) {
            // the main issue here is that the time coordinate will not exist within the RBB for the gatherTags.
            dieWithMessage("Error: using both -timeCoordinate and -gatherTags is not supported.");
        }

        if(filterTagsMulti.size() >0 && this._filter_tags.getItemCount() > 0) {
            System.err.println(
                    "Warning: -filterTags and -filterTagsMulti were used together."+
                    "Consider using just -filterTagsMulti with values specified for some tags,"+
                    "unless you really wanted these options to specify separate tagsets."
                    );
        }

        for(String t : filterTagsMulti) {
            Tagset[] a = Event.findTagCombinations(rbb.db(), t);
            TagsetComparator compare = new TagsetComparator();
            compare.compareNumbersAsNumbers(a);
            Arrays.sort(a, compare);
            for(Tagset t0 : a)
                this._filter_tags.addItem(t0.toString());
        }

        this.tsCache = new EventCache(rbb);
        this.flagCache = new EventCache(_predictionsRBB);

       EventQueue.invokeLater(new Runnable() {
                      public void run() {
                          try {
                            createUI();
                          } catch(Exception e) {
                              System.err.println("Error in CreateUI: "+e);
                          }
                      }
                   });
    }

    /**
     * Creates the User Interface Swing components.
     * This must be called from the Swing thread
     */
    void createUI() throws Exception {
        // Make a _frame
        _frame = new JFrame("Draw Timeseries");

        _frame.addComponentListener(new ComponentAdapter() {
            public void componentResized(ComponentEvent e) {
                repaint(true);
                }});

        _frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        Container framecontent = _frame.getContentPane();

        // Give it a gridbag layout
        GridBagLayout gridbag = new GridBagLayout();
        framecontent.setLayout(gridbag);

        ////////// Construct Toolbar
        JToolBar toolbar = new JToolBar();

        JMenuBar mbar = new JMenuBar();
        JMenu menuEdit = new JMenu("Edit");
        mbar.add(menuEdit);
        JMenu menuEditSelect = new JMenu("Select");
        menuEdit.add(menuEditSelect);
        menuEditSelect.add(new AbstractAction("De-select all") {
            @Override public void actionPerformed(ActionEvent e) {
                try {
                    selections.deselectAll();
                } catch (SQLException ex) {
                    JOptionPane.showMessageDialog(_panel, ex.getMessage(), "Error ", JOptionPane.ERROR_MESSAGE);
                }
            }});
        menuEditSelect.add(new AbstractAction("By ID...") {
            @Override public void actionPerformed(ActionEvent e) {
                selectEventsByIDDialog();
            }});
        menuEditSelect.add(new AbstractAction("By Tags...") {
            @Override public void actionPerformed(ActionEvent e) {
                selectEventsByTagsDialog();
            }});

        menuEdit.add(new AbstractAction("Delete...") {
            @Override public void actionPerformed(ActionEvent e) {
                deleteSelectedEventsDialog();
            }});
        menuEdit.add(new AbstractAction("Add/Change Tags...") {
            @Override public void actionPerformed(ActionEvent e) {
                try { setTagsDialog(selections.getSelectedEventIDs()); } catch(SQLException ex) { }
            }});
        menuEdit.add(new AbstractAction("Remove Tags...") {
            @Override public void actionPerformed(ActionEvent e) {
                removeTagsDialog();
            }});
        menuEdit.add(new AbstractAction("Clone...") {
            @Override public void actionPerformed(ActionEvent e) {
                try { cloneDialog(selections.getSelectedEventIDs()); } catch(SQLException ex) { }
            }});
        menuEdit.add(new AbstractAction("Print Selections to Console") {
            @Override public void actionPerformed(ActionEvent e) {
                printSelections(System.out);
            }});
        menuEdit.add(new AbstractAction("Truncate Start of Timeseries") {
            @Override public void actionPerformed(ActionEvent e) {
                truncateTimeseriesDialog("Start");
            }});
        menuEdit.add(new AbstractAction("Truncate End of Timeseries") {
            @Override public void actionPerformed(ActionEvent e) {
                truncateTimeseriesDialog("End");
            }});
        menuEdit.add(new AbstractAction("Set Play Rate...") {
            @Override public void actionPerformed(ActionEvent e) {
                setPlayRateDialog();
            }});

        _frame.setJMenuBar(mbar);

        _playButton = new JToggleButton(new ImageIcon(ImageIO.read(getClass().getResourceAsStream("/gov/sandia/rbb/ui/images/PlayIcon.png")))) { {
                setToolTipText("Play (Spacebar).  Left and Right arrow keys jump time.");
                addItemListener(new ItemListener() {
                    public void itemStateChanged(ItemEvent e) {
                        playCheckedOrUnchecked(e);
                    } }); } };
        toolbar.add(_playButton, true);

        _drawPaths = new JToggleButton(new ImageIcon(ImageIO.read(getClass().getResourceAsStream("/gov/sandia/rbb/ui/images/ShowPathIcon.png"))));
        _drawPaths.setToolTipText("Show the entire path (not just the current location)");
        _drawPaths.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                repaint(true);
            }
        });
        if(showPathsInitially)
            _drawPaths.setSelected(true);
        toolbar.add(_drawPaths);

        _historyLength = new RBBValues(_coordinationRBB, "historyLength");
        _historyLength.setDefault("before", 0f);
        _historyLength.setDefault("after", 0f);

        _historyLength.addEventListener(new RBBEventListener.Adapter(){
            @Override public void eventDataAdded(RBB rbb, DataAdded ec) {
                repaint(false);
            }
        });

        _drawHistory = new JToggleButton(new ImageIcon(ImageIO.read(getClass().getResourceAsStream("/gov/sandia/rbb/ui/images/ShowHistoryIcon.png"))));
        _drawHistory.setToolTipText("Show History - display samples on the path before/after the current time");
        _drawHistory.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                try {
                    if(_drawHistory.isSelected() && _historyLength.getFloats("before")[0]==0f && _historyLength.getFloats("after")[0]==0f)
                        _historyLength.setFloats("before", 1f);
                } catch(SQLException ex) {
                }
                repaint(false);
            }
        });
        toolbar.add(_drawHistory);

        toolbar.add(new JButton(new ImageIcon(ImageIO.read(getClass().getResourceAsStream("/gov/sandia/rbb/ui/images/FitDataIcon.png")))) { {
                setToolTipText("Fit view to selected data (or all data if none is selected)");
                setBorderPainted(false);
                addActionListener(new ActionListener() {
                    public void actionPerformed(ActionEvent e) {
                        String errmsg=null;
                        try {
                            if(!zoomToFitData())
                                errmsg = "No data match the current Filter Tags";
                        } catch (SQLException ex) {
                            errmsg = ex.getMessage();
                        }
                        if(errmsg!=null)
                            JOptionPane.showMessageDialog(_panel, errmsg, "Error fitting view to data", JOptionPane.ERROR_MESSAGE);
                    } }); } }, true);

        toolbar.add(new JButton(new ImageIcon(ImageIO.read(getClass().getResourceAsStream("/gov/sandia/rbb/ui/images/DeleteIcon.png")))) { {
                setToolTipText("Selectively delete data by tags or RBB ID");
                if(rbb.db().isReadOnly()) {
                    setToolTipText("Delete is disabled because the RBB is read-only");
                    setEnabled(false);
                }
                setBorderPainted(false);
                addActionListener(new ActionListener() {
                    public void actionPerformed(ActionEvent e) {
                        deleteSelectedEventsDialog();
                    } }); } }, true);

//        toolbar.add(new JButton(new ImageIcon(ImageIO.read(getClass().getResourceAsStream("/gov/sandia/rbb/ui/images/SetTags.png")))) { {
//                setToolTipText("Set Tags: assign name=value pairs to selected Timeseries");
//                if(rbb.db().isReadOnly()) {
//                    setToolTipText("Set Tags is disabled because the RBB is read-only");
//                    setEnabled(false);
//                }
//                setBorderPainted(false);
//                addActionListener(new ActionListener() {
//                    public void actionPerformed(ActionEvent e) {
//                        setTagsDialog();
//                    } }); } }, true);


        ////////// mouse mode button group
        toolbar.add(new JToolBar.Separator());
        ButtonGroup mouseModeGroup = new ButtonGroup();
        _mouseModeButtons = new HashMap<MouseMode, JToggleButton>();
        for(MouseMode mode : MouseMode.values()) {
            if(mode == MouseMode.NONE)
                continue;
            JToggleButton btn = new JToggleButton(new ImageIcon(ImageIO.read(getClass().getResourceAsStream("/gov/sandia/rbb/ui/images/"+mode.iconName))));
            btn.setToolTipText(mode.getToolTipText());
            if(mode == MouseMode.DRAW && rbb.db().isReadOnly()) {
                btn.setToolTipText("Drawing is disabled because the RBB is read-only");
                btn.setEnabled(false);
            }
            mouseModeGroup.add(btn);
            toolbar.add(btn);
            _mouseModeButtons.put(mode, btn);
        }
        _mouseModeButtons.get(MouseMode.SELECT).setSelected(true);
        toolbar.add(new JToolBar.Separator());

        //// filter tags
        toolbar.add(new JLabel("Show:"));
        _filter_tags.setEditable(true);
        if(_filter_tags.getItemCount()==0) {
            _filter_tags.addItem("type=position");
            _filter_tags.addItem(""); // show everything.
        }
        _filter_tags.setSelectedIndex(0);
        toolbar.add(_filter_tags);

        // redraw screen when timeseries matching filter tags are changed / added / etc
        eventChangeUI = new RBBEventUI(100, 200) {
            @Override public void rbbEventUI(RBBEventChange[] changes) {
                // It might just be telling us about the timeseries we ourselves are currently drawing,
                // which is handled differently (by accumulating to the pathImageCache instead of redrawing all paths)
                // But if any of the changes are to other timeseries, then redraw.
                for(RBBEventChange c : changes) {
                    if(_mouseDrawingTimeseries != null && _mouseDrawingTimeseries.getID().equals(c.event.getID()))
                        continue;
                    // System.err.println("Repaint because of " + changed.length + " changes, starting with " + c.event);
                    repaint(true);
                    return;
                }
            }
        };
        tsCache.addEventListener(eventChangeUI);

        // redraw screen when the filterTags themselves are changed using the GUI
        _filter_tags.addActionListener (new ActionListener () {
            @Override
            public void actionPerformed(ActionEvent e) {
                try {
                    // leaving data selected that's no longer shown can be confusing.
                    selections.deselectAll();

                    // record the event of new filterTags being selected.
                    // other applications may listen for these and choose to change what they display to stay in sync.
                    H2SEvent.delete(_coordinationRBB.db(), byTags("filterTags"));
                    Tagset tags = new Tagset();
                    tags.add("filterTags", getFilterTags().toString());
                    final Double now = System.currentTimeMillis()/1000.0;
                    // setting the tags in the RBB applies them to this application too.
                    new Event(_coordinationRBB.db(), now, now, tags);
                    repaint(true);
                } catch (Exception ex) {
                    System.err.println(ex);
                }
            }
        });

        if(!rbb.db().isReadOnly()) {
            toolbar.add(new JLabel("Create With:"));
            _create_with_tags.setEditable(true);
            // add this default tagset only if no -eventTags are specified.
            if(_create_with_tags.getModel().getSize() == 0) {
              _create_with_tags.addItem("type=position,color=red");
              _create_with_tags.addItem("type=position,color=green");
              _create_with_tags.addItem("type=position,color=blue");
              _create_with_tags.addItem("type=position,color=yellow");
            }
            toolbar.add(_create_with_tags);
        }

        GridBagConstraints constraints = new GridBagConstraints();
        constraints.weightx = 1;
//        constraints.gridwidth = GridBagConstraints.
//        constraints.fill = GridBagConstraints.HORIZONTAL;
        constraints.weighty = 0;
//        framecontent.add(mbar, constraints);
        constraints.weightx = 1;
        constraints.gridwidth = GridBagConstraints.REMAINDER;
        constraints.fill = GridBagConstraints.HORIZONTAL;
        constraints.weighty = 0;
        framecontent.add(toolbar, constraints);
        //	gridbag.addLayoutComponent(toolbar, constraints);

        //// allocate the drawing panel.
        if(panelClass != null) { // use a specified panel class.
            try {
              _panel = (JPanel) Class.forName(panelClass).getConstructor(this.getClass(), String.class).newInstance(this, mapType);
            } catch(InvocationTargetException e) {
                throw new Exception("InvocationTargetException: " + e.getMessage());
            }
        }
        else { // make a basic panel in white.

            _panel = new JPanel() {
                @Override
                public void paintComponent(Graphics g) {
                    if(_backgroundImage != null)
                        g.drawImage(_backgroundImage, 0, 0, null);
                    else if(g.getClipBounds()!=null) {
                        g.setColor(Color.WHITE);
                        g.fillRect(g.getClipBounds().x, g.getClipBounds().y, g.getClipBounds().width, g.getClipBounds().height);
                    }
                    try {
                        if(isDrawingPaths())
                            drawPaths((Graphics2D)g);
                        draw(g);
                    }
                    catch(Exception e) {

                    }
                }
            };
        }

        _panel.addMouseMotionListener(this);
        _panel.addMouseListener(this);
        _panel.addMouseWheelListener(this);
        _panel.addKeyListener(this);
        _panel.setPreferredSize(new Dimension(1000,700));

        if(_panel instanceof View)
            _view = (View) _panel;
        else _view = new DefaultView(_panel);
        constraints.fill = GridBagConstraints.BOTH;
        constraints.weighty = 1;
        framecontent.add(_panel, constraints);
        //gridbag.addLayoutComponent(panel, constraints;

        //// another little panel for getTime
        JPanel timePanel = new JPanel();
        constraints.weightx = constraints.weighty = 0;
        framecontent.add(timePanel, constraints);
        timePanel.setLayout(new GridBagLayout());

        constraints.gridwidth = 1;

        timePanel.add(new JLabel("Time:"), constraints);
        _timeText = new JTextField();
        _timeText.setColumns(10); // this is just enough to usefully show time if it is seconds unix epoch
        timePanel.add(_timeText, constraints);

        timePanel.add(new JLabel("Time Step:"), constraints);
        _timeStepText = new JTextField();
        _timeStepText.setColumns(4);
        _timeStepText.setText(Double.toString(_timeStepMs / 1000.0));
        timePanel.add(_timeStepText, constraints);

        // Put in a getTime scroll.
        _timeScroll = new JScrollBar(JScrollBar.HORIZONTAL) { {
                addAdjustmentListener(new AdjustmentListener() {
                    public void adjustmentValueChanged(AdjustmentEvent e) {
                        timeScrollMoved(e);
                    }; }); } };

        _timeScroll.setValues(0, 0, 0, 1000000);
        _timeScroll.addMouseWheelListener(this);

        constraints.weightx = 1;
        constraints.fill = GridBagConstraints.HORIZONTAL;
        constraints.gridwidth = GridBagConstraints.REMAINDER;
        timePanel.add(_timeScroll, constraints);
        //	gridbag.addLayoutComponent(timeScroll, constraints);

        // synchronize getTime with other TimeControl clients
        _replayControl = new RBBReplayControl(_coordinationRBB, this, (int) _timeStepMs);

        // If the user specified an initial time or play rate send them out.
        // If either initialTime or playRate is null, replayControl ignores them individually.
        if(initialTime != null)
            setSimTime(initialTime);

        setTimeScrollAndText();

        _frame.pack();
        // System.err.println("initial panel size: "+_panel.getWidth()+","+_panel.getHeight());

           if(_initialZoom != null && _panel.getWidth()>0) {
               if(!_initialZoom.isEmpty())
                   _view.zoom(_initialZoom); // user-specified initial zoom
               else
                 if(!zoomToFitData()) // if false, there is no currently visible data.
                     _view.zoom(new Rectangle2D.Double(0,0,100,100)); // so just zoom arbitrarily.
               _initialZoom = null; // use initialZoom only once.
           }


         _playButton.setSelected(_replayControl.isPlaying());

        _frame.setVisible(true);


        //_historyLength.setMaximumSize(_historyLength.getSize());
    }

    String getFilterTags() {
        // note: if _filter_tags is null, you have called this from the constructor before _filter_tags was set.
        return _filter_tags.getSelectedItem().toString();
    }

    void setTimeScrollAndText() {
        Long ms = _replayControl.getSimTime();
        Double sec = ms / 1000.0;
        _timeScroll.setValue((int) (1000000*(sec-_minTime)/(_maxTime-_minTime)));
        setTimeText(sec);

    }

    void setTimeText(Double sec) {
        _timeText.setText(String.format("%.9g", sec));
    }

    public static void main(String[] args)
    {
        try
        {
            DrawTimeseries draw = new DrawTimeseries(args);
        }
        catch (Throwable e)
        {
            System.err.println(e.toString());
            System.exit(-1);
        }
    }

    public void playCheckedOrUnchecked(ItemEvent e)
    {
        try {
            if (e.getStateChange() == e.SELECTED) {
                _timeStepMs = (int) (1000 * Double.parseDouble(_timeStepText.getText()));
                _replayControl.setAnimation((int)(_timeStepMs));
                _replayControl.setPlayRate(_playRate);
                _prevDrawTime = null;
            }
            else if (e.getStateChange() == e.DESELECTED) {
               _replayControl.setPlayRate(0);
               setTimeScrollAndText();
            }
        } catch(SQLException ex) {
            System.err.println("DrawTimeseries.playCheckedOrUnchecked Exception: " + ex);
        }
    }

    Point2D toScreen(Float[] xy) { 
        return _view.toScreen(new Point2D.Double(xy[0], xy[1]));
    }

    Point2D toScreen(Timeseries ts, double t) throws SQLException { return toScreen(ts.value(t)); }

    Rectangle2D toScreen(Rectangle2D r) {
        Point2D p = _view.toScreen(new Point2D.Double(r.getMinX(), r.getMinY()));
        Point2D q = _view.toScreen(new Point2D.Double(r.getMaxX(), r.getMaxY()));
        Rectangle2D s = new Rectangle2D.Double(p.getX(), p.getY(), 0, 0);
        s.add(q);
        return s;
    };

    /**
     * Draw a rect from _mouseClicked to the specified point in XOR mode (so drawing twice erases it).
     */
    private void drawXORRect(Point p) {
        if(_mouse_pressed == null)
            return;
        Graphics g = _panel.getGraphics();
        g.setColor(Color.BLACK);
        g.setXORMode(_panel.getBackground());
        Rectangle r = new Rectangle(_mouse_pressed);
        r.add(p);
        g.drawRect(r.x, r.y, r.width, r.height);
    }

    /**
     * Create an RBB Image Event "type=dataSelection" that's a screenshot of the selected entities.
     */
    private ImageEvent dataSelectionSnapshot = null;
    private Timer dataSelectionSnapshotTimer = new Timer(1000, new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                snapshotSelectedEntities();
            }
        });


    void snapshotSelectedEntities() {
        if(this._frame == null || !this._frame.isVisible())
            return;

        try {
            // the tagset for the snapshot event
            Tagset snapshotTags = new Tagset("type=dataSelection");

            final double t = _replayControl.getSimTime() / 1000.0;

            // see if we already have an up-to-date snapshot.
            if(dataSelectionSnapshot != null && dataSelectionSnapshot.getStart() == t) {
                // System.err.println("DrawTimeseries already had snapshot of selected entities.");
                return;
            }

            // make sure we don'evt have an old snapshot lying around.
            H2SEvent.delete(_coordinationRBB.db(), byTags(snapshotTags));
            dataSelectionSnapshot = null;

           // find the bounding box of all selected events currently shown.
           DataRange range = getDataRange(true, false);
           if(range==null)
               return; // no selected data.

           Rectangle2D bbox = toScreen(range.bbox);

           // sanity check.  The screenshot can get crazy if a selected entity is way offscreen
           bbox = bbox.createIntersection(new Rectangle2D.Double(0,0,_panel.getWidth(), _panel.getHeight()));

           // store a screenshot of the selected area.
           final int border=65;
           BufferedImage img = new BufferedImage((int)bbox.getWidth()+2*border, (int)bbox.getHeight()+2*border, BufferedImage.TYPE_3BYTE_BGR);
           Graphics2D g = img.createGraphics();
           g.translate(-bbox.getMinX()+border, -bbox.getMinY()+border);
           _panel.paint(g);

           dataSelectionSnapshot = ImageEvent.createEvent(_coordinationRBB, t, t, snapshotTags, img, "png");

           // System.err.println("DrawTimeseries took snapshot of " + selected.length + " selected entities between ("+minx+","+miny+") and ("+maxx+","+maxy+")");

        }
        catch (Throwable ex)
        {
            System.err.println("DrawTimeseries failed to snapshot selected entities: " + ex);
        }
    }


    /**
     * Update the bounding box specified by min/max x/y as necessary to include x,y
     * If start is not null, end must not be null.
     * If x is not null, y must not be null.
     */
    private class DataRange {
        Rectangle2D bbox;
        Double minTime, maxTime;
        void addPoint(Point2D p) {
            if(bbox==null)
                bbox = new Rectangle2D.Double(p.getX(), p.getY(), 0.0, 0.0);
            else
                bbox.add(p);
        }
        void addTime(double t) {
            if(minTime == null || t < minTime)
                minTime = t;
            if(maxTime == null || t > maxTime)
                maxTime = t;
        }
        void addSample(Sample s) {
            Float[] x = s.getValue();
            addPoint(new Point2D.Double(x[0], x[1]));
            addTime(s.getTime());
        }
        void addSamples(Sample[] as) {
            for(Sample s : as)
                addSample(s);
        }
        @Override
        public String toString() {
            if(bbox==null)
                return null;
            return "xmin="+bbox.getMinX()+" ymin="+bbox.getMinY()+" xmax="+bbox.getMaxX()+" ymax="+bbox.getMaxY()+" start="+minTime+" end="+maxTime;
        }

    }

    /**
     * return the DataRange for data matching the filter tags.
     * If selected is true, only selected data is included.
     * If paths is true, includes the range of visible data for all time.
     *
     * If no data is currently showing, null is returned.
     * If null is not returned, none of the elements of the returned DataRange are null.
     */
    DataRange getDataRange(boolean selected, boolean paths) throws SQLException {
        DataRange result = new DataRange();
        Double searchTime = paths ? null : getSimTime();
        for (Timeseries ts : findTimeseries(selected, searchTime, searchTime)) {
            if(ts.getDim() < 2) {
                warnOnce.println("Warning: the current filter tags ("+getFilterTags()+") include the 1-dimensional timeseries "+ts+"; consider a more restrictive filterTags.");
                continue;
            }
            if(paths)
                result.addSamples(ts.getSamples());
            else
                result.addSample(new Sample(searchTime, ts.value(searchTime)));
        }
        if(result.bbox==null)
            return null;
        return result;
    }

    /**
     * Fit the zoom and timespan to the currently-visible data,
     * Also sets the current time to the start of the data if the current time wasn't within the timespan of the data.
     *
     * returns true if at least 1 data point is visible.
     * throws exception if there's an error getting the data.
     */
    protected boolean zoomToFitData() throws SQLException {
        boolean paths = true; // zoom to fit the visible timeseries across time.
        DataRange r = getDataRange(true, paths);
        if(r==null) { // there is no currently-visible selected data
            r = getDataRange(false, paths);
        }
        if(r == null) {
            System.err.println("zoomToFitData command ignored because no data is visible.");
            return false;
        }

        _minTime = r.minTime;
        _maxTime = r.maxTime;
        if(_maxTime <= _minTime)
            _maxTime = _minTime + 1;

        System.err.println("Fitting view to data range: "+r);

        _view.zoom(r.bbox);

        // also make sure the current time is such that we can see data.
        double now =  getSimTime();
        if(now < _minTime || now > _maxTime)
            setSimTime(_minTime);

        repaint(true);

        return true;
    }

    private void selectEventsByIDDialog() {
        try {
            String instructions = "Enter a space-separated list of Event IDs, or ranges of IDs\n"+
                                   "Example: 57 63 99-104";

            String str = (String) JOptionPane.showInputDialog(
                                _panel,
                                instructions,
                                "Select Events",
                                JOptionPane.PLAIN_MESSAGE,
                                null,
                                null,
                                "");
            if (str == null)
                return;
            if(str.equals(""))
                throw new Exception("No IDs were specified.");

            Set<Long> idSet = new HashSet<Long>();

            for(String chunk : str.split("\\s+")) {
                if(chunk.matches("^\\d+-\\d+$")) { // range of IDs
                    String[] ids = chunk.split("-");
                    Long a = Long.parseLong(ids[0]);
                    Long b = Long.parseLong(ids[1]);
                    if(a==null || b==null || !(b>=a))
                        throw new Exception("The range of IDs "+chunk+" is not valid.");
                    if(b-a > 1000)
                        throw new Exception("The range of IDs "+chunk+" is not valid; it can only contain up to 1000 elements.");
                    for(; a <=b; ++a)
                        idSet.add(a);
                }
                else if (chunk.matches("^\\d+$")) {
                    idSet.add(Long.parseLong(chunk));
                }
                else {
                    throw new Exception(chunk + " is not an ID or range of IDs");
                }
            }

            Long[] idArray = idSet.toArray(new Long[0]);

            // check validity of IDs
            ArrayList<Long> validIDs = new ArrayList<Long>();
            ArrayList<Long> invalidIDs = new ArrayList<Long>();
            Event[] events = Event.getByIDs(rbb.db(), idArray);
            for(int i = 0; i < idArray.length; ++i) {
                if(events[i] != null)
                    validIDs.add(idArray[i]);
                else
                    invalidIDs.add(idArray[i]);
            }

            if(!invalidIDs.isEmpty()) {
                StringBuilder sb = new StringBuilder();
                if(invalidIDs.size() <= 10) {
                    Collections.sort(invalidIDs);
                    sb.append("The following IDs are invalid:");
                    for(Long id : invalidIDs)
                        sb.append(" "+id);
                }
                else {
                    sb.append(""+invalidIDs.size()+" of the IDs were not valid.");
                }
                JOptionPane.showMessageDialog(_panel, sb.toString(), "Warning", JOptionPane.WARNING_MESSAGE);
            }

            if(validIDs.isEmpty()) {
                JOptionPane.showMessageDialog(_panel, "No valid IDs were specified", "Error", JOptionPane.ERROR_MESSAGE);
                return;
            }

            selections.selectEvents(byID(validIDs.toArray(new Long[0])));

        }
        catch(Exception e) {
            JOptionPane.showMessageDialog(_panel, e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
        }
    }


    private void selectEventsByTagsDialog() {
        try {
            String instructions = "Enter a tagset\n"+
                                  "Example: name1=value1,name2=value2";

            String tagset = (String) JOptionPane.showInputDialog(
                                _panel,
                                instructions,
                                "Select Events",
                                JOptionPane.PLAIN_MESSAGE,
                                null,
                                null,
                                "");
            if (tagset == null)
                return;
            if(tagset.equals(""))
                throw new Exception("No tagset was specified.");

            selections.selectEvents(byTags(tagset));
        }
        catch(Exception e) {
            JOptionPane.showMessageDialog(_panel, e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
        }
    }

    private void setTagsDialog(Long... ids) {
        try {
            if(ids.length==0)
                throw new Exception("No Events are selected.");

            String instructions =
                    "Enter the new tags.\n"+
                    "Note: Previous values for these tags will be overwritten, but tags not overwritten will not be removed.\n"+
                    "Example: newTagName=newTagValue";

            String str = (String) JOptionPane.showInputDialog(
                                _panel,
                                instructions,
                                "Specify New Tags",
                                JOptionPane.PLAIN_MESSAGE,
                                null,
                                null,
                                "");

            System.err.println(str);

            if (str == null)
                return; // hit cancel

            if(str.equals(""))
                throw new Exception("No new tags were specified.");

            Tagset newTags = new Tagset(str);

            if(newTags.containsValue(null))
                throw new Exception("The specified tagset has a null tag value.  (A tagset has the form name=value,name2=value2...)");

            for(Long id : ids)
                H2SEvent.setTagsByID(rbb.db(), id, str);

        }
        catch (Exception ex)
        {
            System.err.println("Error Settings Tags: " + ex.getMessage());
            JOptionPane.showMessageDialog(_panel, ex.getMessage(), " Error Setting Tags", JOptionPane.ERROR_MESSAGE);
        }

        repaint(true);

    }

    private void removeTagsDialog() {
        try {
            Long[] ids = selections.getSelectedEventIDs();

            if(ids.length==0)
                throw new Exception("No Events are selected.");

            String instructions =
                    "Enter the names of tags to remove from selected events.\n"+
                    "Example: tagName1,tagName2";

            String str = (String) JOptionPane.showInputDialog(
                                _panel,
                                instructions,
                                "Specify Tags to be Removed",
                                JOptionPane.PLAIN_MESSAGE,
                                null,
                                null,
                                "");

            System.err.println(str);

            if (str == null)
                return; // hit cancel

            if(str.equals(""))
                throw new Exception("No tags were specified for removal.");

            H2SEvent.removeTags(rbb.db(), byID(ids).toString(), str);

        }
        catch (Exception ex)
        {
            System.err.println("Error Settings Tags: " + ex.getMessage());
            JOptionPane.showMessageDialog(_panel, ex.getMessage(), " Error Setting Tags", JOptionPane.ERROR_MESSAGE);
        }

        repaint(true);

    }

    private void setPlayRateDialog() {
        try {
            String instructions =
                    "Specify how fast time will proceed when Play is selected:";

            String str = (String) JOptionPane.showInputDialog(
                                _panel,
                                instructions,
                                "Specify Play Rate",
                                JOptionPane.PLAIN_MESSAGE,
                                null,
                                null,
                                _playRate.toString());

            if (str == null)
                return; // hit cancel

            _playRate = Double.parseDouble(str);
        }
        catch (Exception ex)
        {
            System.err.println("Error Settings Play Rate: " + ex.getMessage());
            JOptionPane.showMessageDialog(_panel, ex.getMessage(), " Error Setting Play Rate", JOptionPane.ERROR_MESSAGE);
        }

        repaint(true);

    }

    /*
     * whichEnd is "Start" or "End"
     */
    private void truncateTimeseriesDialog(String whichEnd) {
        try {
            Long[] ids = selections.getSelectedEventIDs();

            RBBFilter filter = new RBBFilter(withTimeCoordinate(_timeCoordinate), byID(ids));

            Double time = getSimTime();

            ArrayList<Event> deleteEvents = new ArrayList<Event>();
            ArrayList<Event> truncateEvents = new ArrayList<Event>();
            // if truncating the start, then 'time' is the end of the time of interest - don't delete or truncate Events starting after that.
            for(Event e : Event.find(rbb.db(), filter, whichEnd.equals("Start") ? byEnd(time) : byStart(time))) {
                if(e.getStart() <= time && e.getEnd() >= time)
                    truncateEvents.add(e);
                else
                    deleteEvents.add(e);
            }

            if(truncateEvents.isEmpty() && deleteEvents.isEmpty())
                throw new Exception("No selected Events would be affected by this action "+time);

            StringBuilder instructions = new StringBuilder();
            instructions.append(truncateEvents.size() + " selected Timeseres will be truncated:");
            if(truncateEvents.size() <= 20) {
                instructions.append(":");
                for(Event e : truncateEvents)
                    instructions.append("\n\t"+e);
            }
            instructions.append("\n\nThe "+whichEnd.toLowerCase()+" time will be set to the current time of "+ time +
                    "\nand samples "+ (whichEnd.equals("Start") ? "before" : "after") + " this time will be erased");

            instructions.append("\n\n"+deleteEvents.size() + " selected Timeseres will be deleted because they are entirely "+(whichEnd.equals("Start") ? "before" : "after")+" the cutoff");
            if(deleteEvents.size() <= 20) {
                instructions.append(":");
                for(Event e : deleteEvents)
                    instructions.append("\n\t"+e);
            }


            if(JOptionPane.OK_OPTION !=
                    JOptionPane.showConfirmDialog(_panel, instructions.toString(), "Truncate Timeseries "+whichEnd, JOptionPane.OK_CANCEL_OPTION))
                return;

            for(Event e : truncateEvents) {
                if(whichEnd.equals("Start")) {
                    e.setStart(rbb.db(), time);
                }
                else {
                    e.setEnd(rbb.db(), time);
                }
            }

            H2SEvent.delete(rbb.db(), byID(Event.getIDs(deleteEvents.toArray(new Event[0]))));
        }
        catch (Exception ex)
        {
            System.err.println("Error Settings Tags: " + ex.getMessage());
            JOptionPane.showMessageDialog(_panel, ex.getMessage(), " Error Setting Tags", JOptionPane.ERROR_MESSAGE);
        }
    }

    private void cloneDialog(Long... ids) {
        try {
            Event[] events = Event.find(rbb.db(), withTimeCoordinate(_timeCoordinate), byID(ids));

            if(events.length==0)
                throw new Exception("No Events are selected.");

            StringBuilder instructions = new StringBuilder();
            instructions.append(events.length + " selected Events will be cloned");
            if(events.length <= 20) {
                instructions.append(":");
                for(Event e : events)
                    instructions.append("\n     "+e);
            }

            if(isDrawingHistory() && (_historyLength.getFloats("before")[0] > 0 || _historyLength.getFloats("after")[0] > 0))
                instructions.append("\n\nNote: History display is enabled, so only the samples included in the history will be cloned.");

            instructions.append("\n\nOptionally, specify tags to set in the cloned Events:");
            instructions.append("\nExample: newTagName1=newTagValue1,newTagName2=newTagValue2");
            instructions.append("\nNote: The clones will inherit all tags not overwritten by the tags you specify.\n");

            String str = (String) JOptionPane.showInputDialog(
                                _panel,
                                instructions.toString(),
                                "Clone Events",
                                JOptionPane.PLAIN_MESSAGE,
                                null,
                                null,
                                "");

            System.err.println(str);

            if (str == null)
                return; // hit cancel

            Tagset setTags = new Tagset(str);

            if(setTags.containsValue(null))
                throw new Exception("The specified tagset has a null tag value.  (A tagset has the form name=value,name2=value2...)");

            final double now = getSimTime();

            for(Event e : events) {
                Double newStart=null, newEnd=null;

                if(isDrawingHistory() && (_historyLength.getFloats("before")[0] > 0 || _historyLength.getFloats("after")[0] > 0)) {
                    newStart = now - _historyLength.getFloats("before")[0];
                    if(newStart < e.getStart())
                        newStart = e.getStart();
                    newEnd = now + _historyLength.getFloats("after")[0];
                    if(newEnd > e.getEnd())
                        newEnd = e.getEnd();
                }

                H2SEvent.clonePersistent(rbb.db(), e.getID(), newStart, newEnd, str);
            }
        }
        catch (Exception ex)
        {
            System.err.println("Error Settings Tags: " + ex.getMessage());
            JOptionPane.showMessageDialog(_panel, ex.getMessage(), " Error Setting Tags", JOptionPane.ERROR_MESSAGE);
        }

        repaint(true);

    }


    protected void deleteSelectedEventsDialog() {
        try {
            Long[] ids = selections.getSelectedEventIDs();

            if(ids.length ==0)
                throw new Exception("No Events are selected.");

            StringBuilder prompt = new StringBuilder();
            if(ids.length > 20)
                prompt.append("This will delete "+ids.length+" events.  Are you sure?");
            else {
                // show the tagsets of events
                Event[] events = Event.find(rbb.db(), RBBFilter.byID(ids)); // unlike getByIDs, this silently ignores invalid selections
                if(events.length==0)
                    throw new Exception("None of the Event selections is valid");
                prompt.append("This will delete " + ids.length+" events:\n\t");
                for(Event e : events)
                    prompt.append("\n\t"+e.getTagset());
                prompt.append("\nAre you sure?");
            }

            if(JOptionPane.YES_OPTION != JOptionPane.showConfirmDialog(_panel, prompt, "Delete Events?", JOptionPane.YES_NO_OPTION))
                return;
   
            H2SEvent.delete(rbb.db(), RBBFilter.byID(ids));

            selections.deselectAll();
        }
        catch (Exception ex) {
            System.err.println("Error Deleting Events: " + ex.getMessage());
            JOptionPane.showMessageDialog(_panel, ex.getMessage(), " Error Deleting Events:", JOptionPane.ERROR_MESSAGE);
        }

        repaint(true);

    }

    protected void timeScrollMoved(AdjustmentEvent e)
    {
            try {
            // If it moved programmatically (not because a person moved it with the mouse), then ignore.
            if (!_timeScroll.getValueIsAdjusting()) {
                return;
            }

            double sec = _minTime+e.getValue()*(_maxTime-_minTime)/1000000.0;
            _replayControl.setSimTime((long)(sec*1000));
            setTimeText(sec);
            repaint(false);
        } catch(SQLException ex) {
            System.err.println("DrawTimeseries.timeScrollMoved Exception: " + ex);
        }
    }

    /**
     * Trigger redraw of the screen.
     * If repaiting is for any reason that might also change the appearance of the Paths, then redrawPaths should be true; otherwise false.
     */
    public void repaint(boolean redrawPaths)
    {
        if(redrawPaths)
            _pathImageCache=null;

        if(_panel != null)
            _panel.repaint(_panel.getBounds());
    }

    /**
     * Convert colorName to a Color object.  The colorName must be either a field in the Color class (e.g. "black")
     * or a hex code (e.g. 0xff8000).
     *
     * If conversion fails, return the default 'defaultColor' (which could also be null)
     *
     * @param colorName
     * @param defaultColorName
     * @return
     */
    static Color colorForString(String colorName, Color defaultColor) {

        if (colorName == null)
            return defaultColor;

        // see if the color is a named color in java.awt.Color
        try {
            java.lang.reflect.Field field = Class.forName("java.awt.Color").getField(colorName);
            Color color = (Color) field.get(null);
            if (color != null)
                return color;
        }
        catch (Exception e11) {
            // fall-through
        }

        // otherwise try decoding as a 24 bit integer, e.g. 0xff8000
        try {
            Color color = Color.decode(colorName);
//            System.err.println("decoded color " + colorName + " to " + color);
            if (color != null)
                return color;
        } catch (Exception e12) {
        }

        return defaultColor;

    }

    /**
     * get all the timeseries matching filter tags for the current time,
     * or for any time if time==null.
     * If selectedOnly==true, results are further restricted to timeseries that
     * are restricted.
     */
    protected Timeseries[] findTimeseries(boolean selectedOnly, Double start, Double end) throws SQLException {
        tsCache.initCache(byTags(getFilterTags()), withTimeCoordinate(_timeCoordinate));

        RBBFilter currentFilter = byTime(start, end);

        // note that non-visible timeseries may be selected, due to selection generalization.
        // but by initializing the cache with the filter tags, then further restricting
        // by time and selection state if necessary, we get only the ones we want.

        if(selectedOnly)
            currentFilter.also(byID(selections.getSelectedEventIDs()));

        return tsCache.findTimeseries(currentFilter);
            
    }

    /*
     * This is split out from drawPaths because it is normally called from drawPaths
     * to re-generate _pathImageCache, but is also called while the user
     * is interactively drawing a new line.
     */
    private void drawPath(Graphics2D g, Timeseries ts, boolean drawAsSelected) throws SQLException {
        if(drawAsSelected) {
            BasicStroke _selectedStroke = new BasicStroke(2.0f, BasicStroke.CAP_BUTT, BasicStroke.JOIN_ROUND, 1.0f, new float[]{7,7}, 0.0f);
            g.setStroke(_selectedStroke);
            g.setColor(_selectedColor);
        }
        else {
            g.setColor(colorForString(ts.getTagset().getValue("color"), _dotColor));
        }
        
        Sample[] s = ts.getSamples();
        int[] x = new int[s.length];
        int[] y = new int[s.length];
        for(int i = 0; i < s.length; ++i) {
            Point2D p = toScreen(s[i].getValue());
            x[i] = (int) p.getX();
            y[i] = (int) p.getY();
        }

        g.drawPolyline(x, y, s.length);
    }

    /**
     * This function is for implementing 2-pass drawing of paths so selected paths will be on top.
     *<p>
     * if selectedPathsOnly is false, all lines and dots are drawn.
     *<p>
     * If selectedPathsOnly is true, only selected paths (and no dots) are drawn, using _selectedColor
     */
    private void drawPaths(Graphics2D g, boolean selectedPathsOnly) throws SQLException {
        for (Timeseries ts : findTimeseries(selectedPathsOnly, null, null))
        try {
            drawPath(g, ts, selectedPathsOnly);
        } catch(Exception e) {
            warnOnce.println(e+"\nError drawing path for timeseries "+(ts==null?"(null)":ts.toString())+".  If it shouldn't be drawn, consider a more restrictive filterTags.");
        }

    }

    /**
     * This is an image with a transparent background, with only the paths drawn.
     * Whenever any change is made to the program that would change the appearance of the paths, this should be set to null.
     */
    private Image _pathImageCache;

   /*
     * Draw the paths for all times.
     * Normally this should not be called unless _drawPaths is selected
     */
    public void drawPaths(Graphics2D g) throws Exception {
        if(_pathImageCache == null) { // doesn't exist yet; create it.
            // System.err.println("Regenerating path image; t="+System.currentTimeMillis());
            _pathImageCache = new BufferedImage(_panel.getWidth(), _panel.getHeight(), BufferedImage.TYPE_INT_ARGB);
            Graphics2D ig = (Graphics2D) _pathImageCache.getGraphics();
            drawPaths(ig, false);
            drawPaths(ig, true);
            ig.dispose();
        }

        g.drawImage(_pathImageCache, 0, 0, null);
    }

    /**
     * Draw the timeseries for the current time
     */
    public void draw(Graphics g) throws Exception {
        final int defaultSize = 20;
        ResultSet rs_seq;

        final Double now = _replayControl.getSimTime() / 1000.0;

        double drawBefore = 0;
        double drawAfter = 0;
        if(_drawHistory.isSelected()) {
            drawBefore = _historyLength.getFloats("before")[0];
            drawAfter = _historyLength.getFloats("after")[0];
        }

        // find only the currently-visible timeseries.
        Timeseries[] timeseries = findTimeseries(false, now-drawBefore, now+drawAfter);

        //// apply gatherTags
        // this may augment the tags for the timeseries.  tsTags will hold the tagsets, or augmented copies where necessary.
        Map<Timeseries, Tagset> tsTags = new HashMap<Timeseries, Tagset>();
        for(GatherTags gt : this.gatherTags) {
            for(Timeseries ts : timeseries)
                try {
                    tsTags.put(ts, gt.gatherTags(now, ts.getTagset()));
                    // System.err.println("Created gatherTags " + tsTags.get(ts));
                } catch(SQLException e) {
                }
        }
        for(Timeseries ts : timeseries)
            if(tsTags.get(ts) == null)
                tsTags.put(ts, ts.getTagset()); // default - just use the un-augmented tags.


        //// icons
        // In drawing icons, the icons (rather than Timeseries) are the outer loop because Icons are drawn in order, for two reasons.
        // First, so stacking order can be specified by the order of -icon args.
        //    e.g. All airplane icons should be drawn on top of all ship icons.
        // Second, it allows ordering icons so the more specific ones are tested first, working down to icons with very general tagsets.
        //    You can even specify a default icon with an empty tagset to use for whatever doesn't match anything else.
        // So, we need to keep track of which timeseries have already been drawn with an icon.
        Set<Long> tsIconDrawn = new HashSet<Long>();
        for(Icon icon : this.icons) {
            for(Timeseries ts : timeseries) try {
                if(ts.getStart() > now || ts.getEnd() < now) // this is possible, because ts includes timeseries whose history is all that will show.
                    continue;
                if(tsIconDrawn.contains(ts.getID()))
                    continue;
                if(!icon.tags.isSubsetOf(tsTags.get(ts)))
                    continue;
                tsIconDrawn.add(ts.getID());

                double delta = 0.5; // timestep used for estimating heading.
                Point2D pos = toScreen(ts, now);
                Point2D prev = toScreen(ts, Math.max(now-delta, ts.getStart()));
                Point2D future = toScreen(ts, Math.min(now+delta, ts.getEnd()));

                AffineTransform atrans = AffineTransform.getTranslateInstance(pos.getX(), pos.getY());
                atrans.rotate(Math.atan2(future.getY()-prev.getY(), future.getX()-prev.getX()));
                atrans.scale(_iconScale, _iconScale);
                atrans.translate(-icon.img.getWidth()/2, -icon.img.getHeight()/2);
                ((Graphics2D)g).drawImage(icon.img, atrans, null);
             } catch(Exception e) {
                warnOnce.println("Error drawing icon "+icon.tags.toString()+ " on timeseries "+ts.toString()+".  If it shouldn't be drawn, consider a more restrictive filterTags.  "+e);
             }
        }

        //// draw the dots
        // draw selected timeseries in a second pass so they are visible even if cluttered.
        // NOTE: paths are not drawn here, they are drawn by drawPaths, only when the background image is re-generated.
        for(int drawSelected=0; drawSelected <= 1; ++drawSelected)
        for (Timeseries ts : timeseries) {
            try { // draw each timeseries within a try block so an error with it won't stop others from being drawn.
                // draw selected timeseries in a second pass so they are visible even if cluttered.
                final boolean drawingSelected = drawSelected==1;
                if(drawingSelected != selections.isSelected(ts.getID()))
                    continue;

                Color color = colorForString(tsTags.get(ts).getValue("color"), _dotColor);

                // history
                if(drawBefore > 0 || drawAfter > 0) {
                    final double start = now - drawBefore;
                    final double end = now + drawAfter;
                    final int size = 8; // size for history dots
                    final int minAlpha = 75; // alpha at now-historyLength.  Max is 255.
                    for(Sample s : ts.getSamples(start, end)) {
                        Point2D p = toScreen(s.getValue());
                        final int alpha =  (int) interp(s.getTime(), now, s.getTime() < now ? start : end, 255, minAlpha);
                        g.setColor(new Color(color.getRed(), color.getGreen(), color.getBlue(), alpha));
                        g.fillOval((int)(p.getX() - size / 2), (int)(p.getY() - size / 2), size, size);
                    }
                }

                if(ts.getStart() > now || ts.getEnd() < now) // this is possible, because ts includes timeseries whose history is all that will show.
                    continue;

                // get current estimated position.
                // don't extrapolate a position for the timeseries currently being drawn though.
                Float[] data;
                if(_mouseMode == MouseMode.DRAW && this._mouseDrawingTimeseries != null && this._mouseDrawingTimeseries.getID().equals(ts.getID()))
                   // data = H2STimeseries.valuePrev(rbb.db(), evt.getID(), rbb_time, null);
                    data = ts.valuePrev(now); // timeCoordinate?
                else
                    //data = H2STimeseries.valueLinear(rbb.db(), evt.getID(), rbb_time, null);
                    data = ts.valueLinear(now); // timeCoordinate?

                Point2D p = toScreen(data);

                // determine size for current position dot.
                int size = defaultSize;
                final String drawSizeAsString = ts.getTagset().getValue("draw_size");
                if (drawSizeAsString != null)
                    size = Integer.decode(drawSizeAsString);

                // if this timeseries is selected, highlight with a bigger circle behind.
                if(drawingSelected) {
                    g.setColor(_selectedColor);
                    final int bigSize = size * 4 / 3;
                    g.fillOval((int)(p.getX() - bigSize/2), (int)(p.getY() - bigSize/2), bigSize, bigSize);
                    g.setColor(Color.black);
                    g.drawOval((int)(p.getX() - bigSize/2), (int)(p.getY() - bigSize/2), bigSize, bigSize);
                }

                if(_dotColor != null) {
                    g.setColor(colorForString(tsTags.get(ts).getValue("color"), _dotColor));
                    g.fillOval((int)(p.getX() - size / 2), (int)(p.getY() - size / 2), size, size);
                }

                // construct and draw a label for the timeseries.
                String label = null;
                for(String labelName : _labels) {
                    final Set<String> labelValues = ts.getTagset().getValues(labelName);
                    if(labelValues==null)
                        continue;
                    for(String labelValue : labelValues) {
                        if(label==null)
                            label = new String();
                        else
                            label += " ";
                        label += labelValue;
                    }
                }
                if(label != null) {
                    g.setColor(Color.BLACK);
                    g.drawString(label, (int)p.getX()+12, (int)p.getY());
                }
            } catch(Exception e) {
                warnOnce.println(e+"\nError drawing timeseries "+ts.toString()+".  If it shouldn't be drawn, consider a more restrictive filterTags.");
            }
        }

        // for any tag values ending in ".RBBID" with an integer value, draw a line connecting the involved entities.
        flagCache.initCache(byTags("type=flag"), withTimeCoordinate(_timeCoordinate));
        for(Event flag : flagCache.findEvents(byTime(now, now))) {
            Long[] parentIDs = getRBBIDs(flag.getTagset());
            if(parentIDs.length==0)
                continue;
            Set<Point2D> parents = new HashSet<Point2D>();


            for(Timeseries parent : tsCache.getTimeseriesByID(parentIDs)) {
                try {
                    if(parent == null) // if the parent isn't in tsCache, it is not currently being drawn.
                        continue; // this parent event is not currently being drawn, due to filterTags
                    parents.add(toScreen(parent, now));
                } catch(Exception e) {
                    warnOnce.println("Error retrieving timeseries value for event " + parent.getID() + " which is a parent of event " + flag.getID()+": "+e);
                    // error retrieving position of parent event -
                    // for example it might not be a timeseries (just a plain RBB Event)
                    // Or maybe the parentEvent tag contained a non-numeric value for some reason.
                }
            }
            g.setColor(colorForString(flag.getTagset().getValue("color"), _selectedColor));
            if(parents.size()==1) {
                Point2D pos = parents.iterator().next();
                Point2D labelPos = new Point2D.Double(pos.getX()+50, pos.getY()+20);
                g.drawLine((int)pos.getX(), (int)pos.getY(), (int)labelPos.getX(), (int)labelPos.getY());
                g.drawString(flag.getTagset().getValue("model"), (int)labelPos.getX(), (int)labelPos.getY());
            }
            else {
                Point2D prev=null;
                for(Point2D pos : parents) {
                    if(prev != null) {
                        g.drawLine((int)pos.getX(), (int)pos.getY(), (int)prev.getX(), (int)prev.getY());
                        g.drawString(flag.getTagset().getValue("model"), (int)(pos.getX()+prev.getX())/2, (int)(pos.getY()+prev.getY())/2);
                    }
                    prev=pos;
                }
            }
        }

        if(_outputFPS) {
            final long sysTime = System.currentTimeMillis();
            if(_prevDrawTime != null)
                System.err.println("Drawing at " + 1000/(sysTime-_prevDrawTime) + " fps");
            _prevDrawTime = sysTime;
        }

    }

    private Long[] getRBBIDs(Tagset tags) {
        ArrayList<Long> result = new ArrayList<Long>();
        for(String tagName : tags.getNames()) {
            if(!tagName.endsWith(".RBBID"))
                continue;
            for(String tagValue : tags.getValues(tagName)) {
                try {
                    result.add(Long.parseLong(tagValue));
                } catch(NumberFormatException e) {
                    continue;
                }
            }
        }
        return result.toArray(new Long[0]);
    }

    @Override public void replayControl(long simTime, double playRate) {
        if (_replayControl.isPlaying()) {
            if(_mouseMode == MouseMode.DRAW)
                addSampleToDrawing();
//            _timeStepMs = (int) (1000
//                * Double.parseDouble(_timeStepText.getText()));
            this._playRate = playRate;
            _prevDrawTime = null;
        }
        
        setTimeScrollAndText();

        this.repaint(false);
      }

    // print a message to System.err, but only once.
    private PrintStream warnOnce = new PrintStream(System.err) {
        private Set<String> alreadyWarned = new HashSet<String>();
        public void println(String s) {
            if(alreadyWarned.contains(s))
                return;
            System.err.println(s);
            alreadyWarned.add(s);
        }
    };
    
    @Override
    public void mousePressed(MouseEvent e) {
        try {

        if(popupMenu(e))
            return;

        _mouse_pressed = e.getPoint();
        _mouse_pressed_fromscreen = _view.fromScreen(_mouse_pressed);


        // decide what the affect of this mouse press will be.
        if(e.getButton() == MouseEvent.BUTTON3) // BUTTON3 - right click
            _mouseMode = MouseMode.PAN; // special case
        else {
            for(MouseMode m : _mouseModeButtons.keySet())
                if(_mouseModeButtons.get(m).isSelected())
                    _mouseMode = m;
        }

        if(_mouseMode == MouseMode.DRAW && !_replayControl.isPlaying())
            _mouseMode = MouseMode.PAN; // can't draw while not playing, so might as well default to a common, non-data-altering operation.

        // the previous mouse drag location has nothing to do with the current one.
        _mouse_dragged = null;

        } catch(Exception ex) {
            System.err.println(ex);
        }
    }

    @Override
    public void mouseDragged(MouseEvent e) {
        // System.err.println("Mouse Dragged");

        Point prev_mouse_dragged = _mouse_dragged;
        _mouse_dragged = e.getPoint();

        if(_mouseMode == MouseMode.ZOOM || _mouseMode == MouseMode.SELECT){
            if(prev_mouse_dragged != null)
                drawXORRect(prev_mouse_dragged);
            drawXORRect(_mouse_dragged);
        }
        else if(_mouseMode == MouseMode.PAN) {
            // the constraints we try to preserve with PAN:
            //    the data location under the mouse when pressed is still under the mouse
            //    no (cumulative) change to zoom.

            _view.panAndZoom(_mouse_pressed_fromscreen, _mouse_dragged, 0);

            repaint(true);
        }
        else if(_mouseMode == MouseMode.DRAW)
        {
            // This is getSimTime called from tick()
            // The reason for this is because doing it from here would cause
            // periods of no samples when the mouse isn't moving. The software
            // has no problem with this but it leads to jerky motion even when
            // using a smoothing function such as ChaserFE because samples
            // are simply not created.

            // addSampleToDrawing();
        }
    }

    private void addSampleToDrawing() {
        // the MouseMode is set to DRAW by mousePressed, whereas _mouse_dragged
        // isn't set until / unless the mouse is actually dragged.
        // This function might get called in the interim.
        if(_mouse_dragged == null)
            return;

        Long now = _replayControl.getSimTime();
        try
        {
            if (_mouseDrawingTimeseries == null) { // start drawing a timeseries
               _mouseDrawingTimeseries =new Timeseries(rbb, 2, now / 1000.0,
                   TC(_create_with_tags.getSelectedItem().toString()));
            }

            Point2D p = _view.fromScreen(_mouse_dragged);
            _mouseDrawingTimeseries.add(rbb, now / 1000.0, (float)p.getX(), (float)p.getY());

            // instead of triggering re-draw of all paths of all timeseries, just redraw
            // the currently-drawing timeseries to the path image cache.
            // This may not be 100% strictly visually correct, but makes drawing responsive
            if(isDrawingPaths() && _pathImageCache != null)
                this.drawPath((Graphics2D)_pathImageCache.getGraphics(), _mouseDrawingTimeseries, false);

            repaint(false);
        }
        catch (Exception ex)
        {
            System.err.println("Exception in addSampleToDrawing: " + ex);
        }
    }

    /**
     *
     * Adds a 'selected=n' tag (1..n) to targeted data.
     *
     * If the target rect has 0 area, the point nearest its origin is selected, if it is nearby
     * Otherwise all data inside the target are selected.
     *
     */
    private void selectData(Rectangle target) {

        double now = getSimTime();

        // trigger update of data selection snapshot.
        dataSelectionSnapshot = null;

        try {
            // do not use H2STimeseries.findNearest.  It doesn't use our tsCache, so is slow.
            // it also measures distance in data space, not pixel space.
            // furthermore there is no need to re-initialize the cache since we are searching the same events most recently drawn.

//            Set<Long, Timeseries> selected = new TreeMap<Long, Timeseries>();
            Map<Long, Event> selected = new HashMap<Long, Event>();

            // a click selects a single timeseries (or none, if none are nearby), whereas
            // a click/drag rectangle selects multiple timeseries (any inside it)

            if(target.width==0 || target.height==0) { // click = find nearest
                final double thresholdDistance = 10;// this is in pixel space
                Point targetPoint = new Point(target.x, target.y);

                // For a single click the selection preference is:
                // 1) Already-selected timeseries whose current value (dot) is near the click.  (To set the end time of selection, or print the current timeseries value to the console)
                // 2) Already-selected timeseries whose path goes near the click (To set the end time of selection, or move the current time to when the timeseries got there).
                // 3) Any visible timeseries whose current value is near the click
                // 4) Any visible timeseries whose plath goes near the click.

                Timeseries nearest = null;
                Double minDist = null;

                for(int findSelected=1; findSelected >= 0; --findSelected) { // favor already-selected timeseries first.
                    for (Timeseries ts : findTimeseries(findSelected==1, now, now)) {
                        Point2D p0 = toScreen(ts.value(now));
                        Double dist = p0.distance(targetPoint);
                        if(dist <= thresholdDistance && (minDist==null || dist < minDist)) {
                            nearest = ts;
                            minDist = dist;
                        }
                    }

                    if(nearest != null)
                        break;

                    if(!_drawPaths.isSelected())
                        continue; // don't look for paths if they're not being drawn.

                    for (Timeseries ts : findTimeseries(findSelected==1, null, null))
                    for(Sample s : ts.getSamples()) {
                        Point2D p0 = toScreen(s.getValue());
                        Double dist = p0.distance(targetPoint);
                        if(dist <= thresholdDistance && (minDist==null || dist < minDist)) {
                            nearest = ts;
                            minDist = dist;
                        }
                    }

                    if(nearest != null)
                        break;
                }

                if(nearest != null)
                    selected.put(nearest.getID(), nearest);
            }

            else { // the selection rectangle is nonempty - select anything inside it
                if(_drawPaths.isSelected()) { // include samples from any time
                    for (Timeseries ts : findTimeseries(false, null, null))
                    for(Sample s : ts.getSamples())
                        if(target.contains(toScreen(s.getValue()))) {
                            selected.put(ts.getID(), ts);
                            break; // no point repeatedly selecting the same timeseries even if it has several samples within the rectangle.
                        }
                }

                else { // only look at current location
                    for (Timeseries ts : findTimeseries(false, now, now))
                        if(target.contains(toScreen(ts.value(now))))
                            selected.put(ts.getID(), ts);
                }
            }
            
            if(selected.isEmpty()) { // none selected.  This is interpreted as a request do de-select all.
                selections.deselectAll();
                return;
            }

            if(selectionTemplate != null) { // generalize the selection to all Events with the same values for the tags in the template as any explicitly selected timeseries
                for(Event ts : selected.values().toArray(new Event[0])) { // toArray makes a copy so we don't get ConcurrentModificationException by adding while iterating.
                    Tagset selectTagset = Tagset.template(selectionTemplate, ts.getTagset());
                    if(selectTagset.containsValue(null)) {
                        System.err.println("Warning: the user selected a Timeseries that didn't provide all the tags for the -selectionTemplate option.\n\tRequired:\t"+selectionTemplate+"\n\tSelected:\t"+ts.getTagset());
                        continue;
                    }
                    for(Event ev : Event.find(rbb.db(), byTags(selectTagset), withTimeCoordinate(_timeCoordinate)))
                        selected.put(ev.getID(), ev);
                }
            }

            // de-select events selected during a nonempty time interval if the interval
            // does not coincide with when the event exists.
//            ArrayList<Long> deselect = new ArrayList<Long>();
//            for(Event e : selected.values()) {
//                RBBSelection.Selection sel = selections.getSelection(e.getID());
//                if(sel == null)
//                    continue; // this event was not selected
//                double start = Math.min(now, sel.getStart());
//                double end = Math.max(now, sel.getEnd());
//                if(start > e.getEnd() || end < e.getStart())
//                    deselect.add(e.getID());
//            }
//            if(deselect.size() > 0) {
//                selections.deselectEventsByID(_coordinationRBB.db(), deselect.toArray(new Long[0]));
//                for(Long id : deselect)
//                    selected.remove(id);
//            }

            Long[] selectedIDs = selected.keySet().toArray(new Long[0]);
            selections.selectEvents(byID(selectedIDs));
        }
        catch(java.sql.SQLException ex)
        {
            System.err.println("selectDataNearScreenPoint got exception: " + ex.toString());
        }
        finally {
            repaint(true);
        }
    }

    private void printSelections(PrintStream ps) {
        try {
            final double now = getSimTime();
            Event[] events = Event.find(rbb.db(), byID(selections.getSelectedEventIDs()));
            ps.println("There are " + events.length + " selected events:");

            for(Event ev : events) {

                ps.print("Selected: " + ev);

                // get and print current value, if it's a timeseries.
                Float [] currentValue = null;
                try {
                    currentValue = H2STimeseries.value(rbb.db(), ev.getID(), now, _timeCoordinate, null);
                } catch(SQLException e) {
                     // probably it's an Event (not Timeseries) ID.
                }

                if(currentValue != null)
                    ps.print(" at t=" + now + " value=" + StringsWriter.join(",", currentValue));

                ps.println();
            }
        } catch(SQLException e) {
            System.out.println("DrawTimeseries.printSelectionsToStderr Error: "+e);
        }
    }

    @Override
    public void mouseMoved(MouseEvent e) {
        // no-op
    }

    @Override
    public void mouseClicked(MouseEvent e) {
        Point2D p = _view.fromScreen(e.getPoint());
        System.out.println("Click at "+p.getX()+","+p.getY());
   }

    static class ClosestPoint {
        double distance = Double.MAX_VALUE;
        Timeseries timeseries;
        Sample closest;

        static Comparator<ClosestPoint> compareByDistance() {
            return new Comparator<ClosestPoint>() {
                @Override public int compare(ClosestPoint o1, ClosestPoint o2) {
                    return (int) Math.signum(o1.distance - o2.distance);
                }
            };
        }
        
        public static Tagset[] getTagsets(ClosestPoint... p) {
            Tagset[] result = new Tagset[p.length];
            for(int i = 0; i < p.length; ++i)
                result[i] = p[i].timeseries.getTagset();
            return result;
        }
    }

    private double getSimTime() {
        return _replayControl.getSimTime()/1000.0;
    }

    private void setSimTime(double newTime) throws SQLException {
        _replayControl.setSimTime((long) (newTime*1000));
        setTimeScrollAndText();
        repaint(false);
    }

    private ClosestPoint[] findNearbyTimeseries(Point screenPoint) throws SQLException {

        ArrayList<ClosestPoint> results = new ArrayList<ClosestPoint>();

        final double now = getSimTime();
        final double thresholdDistance = 15;// this is in pixel space

        Double startTime = now;
        Double endTime = now;
        if(isDrawingPaths())
            startTime = endTime = null;
        else if(isDrawingHistory()) {
            startTime = now - _historyLength.getFloats("before")[0];
            endTime = now - _historyLength.getFloats("after")[0];
        }

        for(Timeseries ts : findTimeseries(false, startTime, endTime)) {
            ClosestPoint cp = new ClosestPoint();
            cp.timeseries = ts;

            // If the current position is within threshold, don't search any samples before or after
            // for an even closer one... This allows inspecting at the current time exactly.
            if(ts.getStart() <= now && ts.getEnd() >= now) { // might not be true if history or paths are shown.
                Sample currentPosition = new Sample(now, ts.valueLinear(now));
                final double dist = toScreen(currentPosition.getValue()).distance(screenPoint);
                if(dist <= thresholdDistance) {
                    cp.closest = currentPosition;
                    cp.distance = dist;
                    results.add(cp);
                    continue;
                }
            }

            Sample[] samples;
            if(isDrawingPaths())
                samples = ts.getSamples();
            else if(isDrawingHistory())
                samples = ts.getSamples(startTime, endTime);
            else
                continue;

            for(Sample s : samples) {
                Point2D p0 = toScreen(s.getValue());
                final double dist = p0.distance(screenPoint);
                if(dist <= thresholdDistance && dist < cp.distance) {
                    cp.closest = s;
                    cp.distance = dist;
                }
            }

            if(cp.closest != null)
                results.add(cp);
        }

        Collections.sort(results, ClosestPoint.compareByDistance());

        return results.toArray(new ClosestPoint[0]);
    }

    /*
     * Returns true if the MouseEvent calls for a Popup Menu
     */
    private boolean popupMenu(MouseEvent e) throws Exception {
        if(!e.isPopupTrigger())
            return false;

        JPopupMenu popup = new JPopupMenu();

        final double now = getSimTime();

        // find all the timeseries that are nearby
        ClosestPoint[] nearby = findNearbyTimeseries(e.getPoint());

        if(nearby.length == 0)
            return false;

        Tagset sameValuedTags = Tagset.intersection(ClosestPoint.getTagsets(nearby));
        
        // if near multiple timeseries, make each a submenu, otherwise add actions directly to popup.
        JComponent tsMenu = popup;
        for(ClosestPoint cp_ : nearby) {
            final ClosestPoint cp = cp_;

            // only show tags in which the timeseries differ...
            Tagset distinctTags = cp.timeseries.getTagset().clone();
            for(String name : sameValuedTags.getNames())
                for(String value : sameValuedTags.getValues(name))
                    distinctTags.remove(name, value);
            if(distinctTags.getNumTags()==0) // fallback in case the tags are not disinct.
                distinctTags = cp.timeseries.getTagset();
                    
            if(nearby.length > 1)
                tsMenu = new JMenu(distinctTags.toString());
            else
                tsMenu.add(new JMenuItem(distinctTags.toString()));
            // submenu.setMnemonic(KeyEvent.VK_S);

             if(cp.closest.getTime() != now)
                tsMenu.add(new JMenuItem(new AbstractAction("Go to time " + cp.closest.getTime()) {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        try { setSimTime(cp.closest.getTime()); } catch(SQLException ex) { }
                    }
                }));

           if(selections.isSelected(cp.timeseries.getID())) {
              tsMenu.add(new JMenuItem(new AbstractAction("De-Select") {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        try { selections.unSelectEvents(byID(cp.timeseries.getID())); } catch(SQLException ex) { }
                    }
                }));               
           }
           else {
              tsMenu.add(new JMenuItem(new AbstractAction("Select") {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        try { selections.selectEvents(byID(cp.timeseries.getID())); } catch(SQLException ex) { }
                    }
                }));
           }
           
            tsMenu.add(new JMenuItem(new AbstractAction("Clone...") {
                @Override
                public void actionPerformed(ActionEvent e) {
                    cloneDialog(cp.timeseries.getID());
                }
            }));

            tsMenu.add(new JMenuItem(new AbstractAction("Add/Change Tags...") {
                @Override
                public void actionPerformed(ActionEvent e) {
                    setTagsDialog(cp.timeseries.getID());
                }
            }));

             if(cp.closest.getTime() < now)
                tsMenu.add(new JMenuItem(new AbstractAction("Set History Start") {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        _drawHistory.setSelected(true);
                        try { _historyLength.setFloats("before", new Float[]{(float)(now-cp.closest.getTime())}); } catch(SQLException ex) { }
                    }
                }));

            if(cp.closest.getTime() > now)
                tsMenu.add(new JMenuItem(new AbstractAction("Set History End") {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        _drawHistory.setSelected(true);
                        try { _historyLength.setFloats("after", new Float[]{(float)(cp.closest.getTime()-now)}); } catch(SQLException ex) { }
                    }
                }));


            tsMenu.add(new JMenuItem(new AbstractAction("Truncate Start") {
                @Override
                public void actionPerformed(ActionEvent e) {
                    if(JOptionPane.OK_OPTION == JOptionPane.showConfirmDialog(_panel, "Delete the start of the timeseries until time "+cp.closest.getTime()+"?\n\n"+cp.timeseries, "Truncate Timeseries", JOptionPane.OK_CANCEL_OPTION))
                        try { cp.timeseries.setStart(rbb.db(), cp.closest.getTime()); } catch(SQLException ex) { }
                }
            }));

            tsMenu.add(new JMenuItem(new AbstractAction("Truncate End") {
                @Override
                public void actionPerformed(ActionEvent e) {
                    if(JOptionPane.OK_OPTION == JOptionPane.showConfirmDialog(_panel, "Delete the end of the timeseries after time "+cp.closest.getTime()+"?\n\n"+cp.timeseries, "Truncate Timeseries", JOptionPane.OK_CANCEL_OPTION))
                        try { cp.timeseries.setEnd(rbb.db(), cp.closest.getTime()); } catch(SQLException ex) { }
                }
            }));


            tsMenu.add(new JMenuItem(new AbstractAction("Delete") {
                @Override
                public void actionPerformed(ActionEvent e) {
                    if(JOptionPane.OK_OPTION == JOptionPane.showConfirmDialog(_panel, "Delete the timeseries?\n\n"+cp.timeseries, "Delete", JOptionPane.OK_CANCEL_OPTION))
                        try { H2SEvent.deleteByID(rbb.db(),cp.timeseries.getID()); } catch(SQLException ex) { }
                }
            }));


            //            JMenuItem menuItem = new JMenuItem("Select");

            // menuItem.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_2, ActionEvent.ALT_MASK));
            // submenu.add(menuItem);
                
            if(nearby.length > 1)
                popup.add(tsMenu);
        }

        popup.show(e.getComponent(), e.getX(), e.getY());

        return true;
    }

    @Override
    public void mouseReleased(MouseEvent e) {
        try {
        // System.err.println("Mouse Released");

        if(popupMenu(e))
            return;

        else if(_mouseMode == MouseMode.SELECT) {
            if(e.getPoint().equals(_mouse_pressed)) { // mouse never moved
                selectData(new Rectangle(e.getPoint()));
            }
            else if(_mouse_dragged != null) {
                drawXORRect(_mouse_dragged); // erase zoom rect.
                Rectangle r = new Rectangle(e.getPoint());
                r.add(_mouse_pressed);
                selectData(r);
            }
        }

        else if(_mouseMode == MouseMode.ZOOM) {
            Point2D p = _view.fromScreen(_mouse_pressed);
            if(e.getPoint().equals(_mouse_pressed)) { // mouse never moved
                _view.panAndZoom(p, _mouse_pressed, -1); // zoom out
            }
            else if(e.getPoint().distance(_mouse_pressed) >= 5 && // don't zoom in to a tiny rectangle; it's almost certainly an accident.
                    _mouse_dragged != null) {
                drawXORRect(_mouse_dragged); // erase zoom rect.
                Rectangle2D r = new Rectangle2D.Double(p.getX(), p.getY(), 0.0, 0.0);
                r.add(_view.fromScreen(e.getPoint()));
                _view.zoom(r);
            }
            repaint(true);
        }

        else if(_mouseMode == MouseMode.DRAW) {
            _mouseDrawingTimeseries.setEnd(rbb.db(), _replayControl.getSimTime() / 1000.0);
            _mouseDrawingTimeseries = null;
            repaint(true);
        }

        _mouseMode = MouseMode.NONE;

        } catch(Exception ex) {
            System.err.println(ex.getMessage());
        }
    }

    @Override
    public void mouseEntered(MouseEvent e) {
        // without this the hotkeys for MouseMode (shift, ctrl, meta) will not work.
        _panel.requestFocusInWindow();
    }

    @Override
    public void mouseExited(MouseEvent e) {
        // no-op
    }

    int mouseWheelPos = 0;

    @Override
    public void mouseWheelMoved(MouseWheelEvent e) {
        if(e.getSource() == _timeScroll) {
            System.err.println("DrawTimeseries.mouseWheelMoved: " + e);
            mouseWheelPos += (int) Math.pow(e.getWheelRotation(), 3);
            System.err.println("MouseWheelPos " + mouseWheelPos);
            _timeScroll.setVisibleAmount(Math.abs(mouseWheelPos));
        }
        else {
            if(e.getWheelRotation()>0) // > 0 means wheel was turned down/towards the user = zoom out
                _view.panAndZoom(_view.fromScreen(e.getPoint()), e.getPoint(), -1);
            else
                _view.panAndZoom(_view.fromScreen(e.getPoint()), e.getPoint(), 1);
            repaint(true);
        }        
    }

    @Override
    public void keyTyped(KeyEvent e) {
//        System.err.println("keyTyped: "+e);
    }

    @Override
    public void keyPressed(KeyEvent e) {
        // System.err.println("keyPressed: "+e);
        for(MouseMode m : _mouseModeButtons.keySet())
            if(m.keyCode != null && e.getKeyCode() == m.keyCode)
                _mouseModeButtons.get(m).setSelected(true);

        Double newTimeSec = null;
        Double jumpSeconds = 2.0; // number of seconds, in realtime, to jump via arrow keys

        if(e.getKeyCode() == KeyEvent.VK_SPACE)
            _playButton.setSelected(!_playButton.isSelected());
        else if(e.getKeyCode() == KeyEvent.VK_LEFT)
            newTimeSec = getSimTime() - jumpSeconds * _playRate;
        else if(e.getKeyCode() == KeyEvent.VK_RIGHT)
            newTimeSec = getSimTime() + jumpSeconds * _playRate;

        if(newTimeSec != null) {
            try {
                setSimTime(newTimeSec);
            } catch (SQLException ex) {
            }
            setTimeScrollAndText();
            repaint(false);
        }

    }

    @Override
    public void keyReleased(KeyEvent e) {
        // System.err.println("keyReleased: "+e);
        for(MouseMode m : _mouseModeButtons.keySet())
            if(m.keyCode != null && e.getKeyCode() == m.keyCode)
                _mouseModeButtons.get(MouseMode.ZOOM).setSelected(true); // ZOOM is the default mode.
    }


    /*
     * interpolate a from the range a1,a2 to the range b1,b2
     */
    private static double interp(double a, double a1, double a2, double b1, double b2) {
        return b1+(a-a1)*(b2-b1)/(a2-a1);
    }
}

class DefaultView implements DrawTimeseries.View {

    JPanel _panel;
    private Double _xoff = 0.0, _yoff = 0.0, _scale = 1.0; // scale and offset for data points

    DefaultView(JPanel panel) {
        this._panel = panel;
    }

    @Override public void zoom(Rectangle2D r) {
        Float forcedAspect = 1.0f;
        Dimension dim = _panel.getSize();
        _scale = Math.min(dim.width / r.getWidth(), dim.height / r.getHeight());

        // set offsets so the middle of each axis will be in the center.
        _xoff = dim.width / 2 / _scale - (float) r.getCenterX();
        _yoff = dim.height / 2 / _scale - (float) r.getCenterY();
    }

    @Override public Point2D toScreen(Point2D p) {
        return new Point2D.Double(
                (p.getX() + _xoff) * _scale,
                yup((p.getY() +_yoff) * _scale));
    }

    @Override public Point2D fromScreen(Point2D q) {
        return new Point2D.Double(q.getX() / _scale - _xoff,
                yup(q.getY()) / _scale - _yoff);
    }

    @Override
    public void panAndZoom(Point2D dataPoint, Point pixel, int zoomIncrement) {
        if(zoomIncrement != 0) {
            _scale *= Math.pow(2, zoomIncrement);
        }
        _xoff = pixel.getX() / _scale - dataPoint.getX();
        _yoff = yup(pixel.getY()) / _scale - dataPoint.getY();
    }
    /**
     * Make y up (instead of down)
     */
    private double yup(double y) {
//        return y;
        return _panel.getHeight()-1-y;
    }

};
