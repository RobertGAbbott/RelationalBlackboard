/*
 * File:                RBBEventTimeline.java
 * Authors:             Charles Gieseler, Matt Glickman
 * Company:             Sandia National Laboratories
 * Project:             AEMASE Timeline
 *
 * Copyright 2009, Sandia Corporation.
 * Under the terms of Contract DE-AC04-94AL85000, there is a non-exclusive
 * license for use of this work by or on behalf of the U.S. Government. Export
 * of this program may require a license from the United States Government.
 * See CopyrightHistory.txt for complete details.
 *
 * Revision History:
 *
 * $Log: RBBEventTimeline.java,v $
 * Revision 1.9  2013/05/16 22:47:15  rgabbot
 * about to delete
 *
 * Revision 1.8  2013/03/27 19:27:11  rgabbot
 * no message
 *
 * Revision 1.7  2012/12/13 01:14:55  rgabbot
 * Added -timeCoordinate option
 *
 * Revision 1.6  2012/09/08 00:29:40  rgabbot
 * Merge Derek's and my changes.
 *
 * Revision 1.3  2012/08/21 20:00:52  rgabbot
 * Default timeline length was being used even when another value had been specified, so removed the default timeline length variable and retrieve the current value each time instead.
 *
 * Revision 1.2  2012/08/14 23:46:30  rgabbot
 * Now using imports instead of explicit package names for java.sql.Connection and java.sql.ResultSet
 *
 * Revision 1.1  2012/08/07 20:25:17  rgabbot
 * Major code refactor and schema change prior to first public reelase
 *
 * Revision 1.25  2012/07/06 18:02:13  rgabbot
 * Renamed "metric" to "model"
 *
 * Revision 1.24  2012/07/06 17:08:39  rgabbot
 * Enhanced help string with example for AEMASE flags
 *
 * Revision 1.23  2012/07/06 17:03:25  rgabbot
 * Enhanced help string.
 * Renamed -timelineTagset and -multiTimelineTagset options to -timeline and -multiTimeline, although the old options still work for backwards compatibility.
 *
 * Revision 1.22  2012/06/22 17:15:14  dtrumbo
 * Refactored timeline labels to be external to the timeline panel.
 *
 * Revision 1.21  2011/12/23 03:08:12  rgabbot
 * Removed unnecessary thread creation for RBBReplayControlTransport.  RBB.addEventListener starts a thread automatically now.
 *
 * Revision 1.20  2011/12/22 22:46:17  rgabbot
 * Update to latest RBB
 *
 * Revision 1.19  2011/11/03 23:58:47  rgabbot
 * Now using the new  H2SEvent.findTagCombinations method so AEMASE models that have been deleted won't show up any more.
 *
 * Revision 1.18  2011/10/19 19:49:35  dtrumbo
 * Finished multiTimelineTagset option and fixed flashing behavior.
 *
 * Revision 1.17  2011/10/17 17:02:09  dtrumbo
 * Code clean up and arbitrary number of timelines implemented.
 *
 * Revision 1.16  2011/09/14 21:40:35  rgabbot
 * Restored RBBReplayControlTransport functionality.
 *
 * Revision 1.15  2011/09/14 21:19:43  rgabbot
 * Update to latest RBB and updates such as loading / saving models
 *
 * Revision 1.14  2011/09/14 20:44:16  rgabbot
 * Update to latest RBB
 *
 * Revision 1.13  2011/09/13 22:54:14  jhwhetz
 * Fixes to repair errors in Hudson build.
 *
 * Revision 1.12  2011/09/07 14:21:25  rgabbot
 * Update to latest RBB
 *
 * Revision 1.11  2011/09/07 01:23:17  mrglick
 * Added -timespanEnd flag
 *
 * Revision 1.10  2011/09/06 21:30:53  rgabbot
 * Remove classes that now reside in the SandiaInteractiveTimeVizualization project in SVN.
 *
 * Revision 1.9  2011/09/06 19:30:06  mrglick
 * Now with command-line arguments!
 *
 * Revision 1.8  2011/08/25 22:23:42  rgabbot
 * Updated RBB-client code to not use the RBBFactory which has been removed.
 *
 * Revision 1.7  2011/07/28 00:21:09  rgabbot
 * Changed for new package naming in ReplayControl.  Added dependency on ReplayControl project.
 *
 * Revision 1.6  2011/07/26 22:34:19  mrglick
 * Suppress "Add Timeline" button (which doesn't work yet) in favor of "Add RBB Event".
 *
 * Revision 1.5  2011/07/26 21:09:26  mrglick
 * Interval between time markers now automatically resizes to maintain a constant number of markers regardless of time scale changes.
 *
 * Revision 1.4  2011/07/26 20:27:30  mrglick
 * Reorganization.  Timeline/tagset functionality with temporary interface.
 * Still laced with debugging messages.
 *
 * Revision 1.3  2011/06/13 22:30:04  mrglick
 * Added big comment documenting joys/horrors.
 *
 * Revision 1.2  2011/06/13 20:36:31  mrglick
 * Updated to use RBB TCP Client stuff.
 *
 * Revision 1.1  2011/05/26 15:56:53  mrglick
 * First check-in of new generic RBB event timeline for AEMASE.  Very, very prototype-y.
 *
 * Revision 1.2  2010/01/25 20:39:31  cjgiese
 * Added a panel with buttons to load and save timelines from/to file. Note, all buttons are still malfunctioning in that their labels are still not showing up
 *
 * Revision 1.1  2010/01/19 21:27:25  cjgiese
 * Put together a driver to test work through using JLayeredPane and test out the display of the CurrentTimeIndicator. Turned it into a demo. Put it under a new package called gov.sandia.aemase.timeline.demo. Had a cookie.
 *
 *
 */

/**
 * RBBEventTimeline is based on the code from CurrentTimeDemo in the
 * SandiaInteractiveTimelineVisualization.  It is a modification that provides
 * a 1:1 mapping between annotations and RBB events.
 * 
 * The mapping is a bit forced.
 * 
 * RBB events have a start time and an end time, while annotations have a
 * start time and a duration.  These two are equivalent, so there is no problem.
 * 
 * However, annotations also have an author, a type, and note text, and these
 * three are all mapped to the RBB event tagset.  The annotation author field
 * maps to an "author" tag, the annotation notes field maps to a "text" tag,
 * and the rest of the tags are represented in string form in the annotation
 * type field.  (This was before I realized that the point of the type field
 * was to code for the color of the annotation display.  This should be
 * resolved somehow.) This mapping is mediated by eventToAnnotation() and
 * annotationToEventInfo().
 * 
 * One ugly business is that when a new annotation is created by right-clicking
 * in the GUI, it first creates a dummy annotation and then calls
 * requestUpdateAnnotation() to fill in the fields.  This call was hard to
 * intercept given the hierarchical class structure, so the initial annotation
 * created does NOT initially have an RBB event counterpart.  Yuck.
 * 
 * Another bit that is currently ugly (but working) is that every time there is
 * a change to the RBB, ALL annotations are purged and re-created from the RBB!
 * It's offensive, but it works for now.  What this means, by the way, is that
 * any temporary annotations created as described above will be purged upon
 * the next RBB update.
 * 
 * The harsh routine that does this full purge-and-update business is called
 * updateFromRBB().  This routine is currently also where logic should currently
 * be put that decides which events are displayed on which timelines.  This
 * logic will ultimately become something that can be changed at runtime, but
 * there is currently a bug that prevents adding new timelines on the fly.  This
 * bug is inherited from currentTimeDemo, and to see it just run the demo and
 * then try adding an annotation.  As soon as you do this, you can see that
 * one thread of the process goes into a busy loop and eats up one CPU core.
 *
 * Other note:  When you create a new RBB event/annotation by clicking on the
 * "Add RBB Event" button (instead of right-clicking in the GUI), you get a
 * different dialog more directly tailored to an RBB Event than to an
 * annotation.  It also lets you specify the time in milliseconds, which
 * end up getting rounded to the nearest second if you subsequently edit
 * the annotation via the timeline.
 */
package gov.sandia.rbb.ui.timeline;

import gov.sandia.rbb.RBBFilter;
import gov.sandia.cognition.timeline.AnnotatableTimeline;
import gov.sandia.cognition.timeline.DefaultAnnotatableTimeline;
import gov.sandia.cognition.timeline.DefaultTimeline;
import gov.sandia.cognition.timeline.Timeline;
import gov.sandia.cognition.timeline.annotation.Annotation;
import gov.sandia.cognition.timeline.annotation.DefaultAnnotation;
import gov.sandia.cognition.timeline.annotation.view.ColoredMarkerHighlighter;
import gov.sandia.cognition.timeline.annotation.view.DefaultAnnotationMouseAdapter;
import gov.sandia.cognition.timeline.control.AbstractAnnotatableCompoundTimelineController;
import gov.sandia.cognition.timeline.event.action.annotation.CreateAnnotationActionEvent;
import gov.sandia.cognition.timeline.event.action.annotation.DeleteAnnotationActionEvent;
import gov.sandia.cognition.timeline.event.action.annotation.UpdateAnnotationActionEvent;
import gov.sandia.cognition.timeline.event.action.compound.CreateTimelineActionEvent;
import gov.sandia.cognition.timeline.view.AbstractAnnotatableTimelineView;
import gov.sandia.cognition.timeline.view.CommunicationsTimelineView;
import gov.sandia.cognition.timeline.view.DefaultAnnotatableTimelineMouseAdapter;
import gov.sandia.cognition.timeline.view.MarkedIntervalTimelinePanel;
import gov.sandia.cognition.timeline.xml.TimelineFileIO;
import gov.sandia.cognition.timeline.xml.TimelineXMLException;

import gov.sandia.rbb.ui.RBBReplayControl;
import gov.sandia.cognition.timeline.event.action.compound.RemoveTimelineActionEvent;
import gov.sandia.cognition.timeline.view.TimelineView;
import gov.sandia.rbb.Event;
import gov.sandia.rbb.RBB;
import gov.sandia.rbb.RBBEventChange;
import gov.sandia.rbb.RBBEventListener;
import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.impl.h2.statics.H2SEvent;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.EventQueue;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.swing.AbstractAction;
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.Timer;
import static gov.sandia.rbb.RBBFilter.*;

public class RBBEventTimeline
    extends AbstractAnnotatableCompoundTimelineController
{

    // Constants
    private static final long DEFAULT_NUMBER_OF_TIME_MARKERS = 20;
    private static final String NOTAGVALUE = "NO ASSOCIATED TAG VALUE";

    // UI Stuff
    private JPanel timelineManagementPanel;
    private JPanel currentTimeControlPanel;

    // Data backing UI
    private RBB rbb;
    private DefaultAnnotatableTimeline timelineData1;
    
    // All RBB-related information for the timelines.
    private TimelineDataManager dataManager = new TimelineDataManager();
    
    private Timer delayedUpdateTimer = createDelayedUpdateTimer();
    private Set<String> timelinesToUpdate = new HashSet<String>();
    
    private Timer createDelayedUpdateTimer() {
        Timer timer = new Timer(1000, new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                updateFromRBB();
            }
        });
        timer.setRepeats(false);
        return timer;
    }
        
    @Override
    public boolean requestRemoveTimeline(RemoveTimelineActionEvent event) {
        boolean b = super.requestRemoveTimeline(event);
        if(b) {
            rebuildLabelPanel();
        }
        return b;
    }

    @Override
    public boolean requestCreateTimeline(CreateTimelineActionEvent event) {	
        boolean b = super.requestCreateTimeline(event);
        if(b) {
            rebuildLabelPanel();
        }
        return b;
    }
    
    JPanel pnlLabelsInner;
    protected void rebuildLabelPanel() {
        if(pnlLabelsInner != null) {
            pnlLabelsInner.removeAll();
            pnlLabelsInner.setLayout(new GridLayout(getTimelineData().size(), 1));
            List<? extends TimelineView> vs = getTimelineView().getChildTimelineViews();
            for(TimelineView v : vs) {
                JLabel lbl = new JLabel(v.getTimeline().getName());
                lbl.setForeground(getTimelineView().getForeground());
                pnlLabelsInner.add(lbl);
            }
            pnlLabelsInner.updateUI();
        }
    }
	
    /*
     * Display-related code
     */
    private void loadTimelineDataFromFile(File timelinesFile)
    {
        clearTimelines();
        
        try {
            Collection<Timeline> newTimelineData =
                TimelineFileIO.loadTimelinesFromFile(timelinesFile);

            for(Timeline data : newTimelineData) {
                CreateTimelineActionEvent event;
                if (data.getName().contains("Communications Timeline"))
                {
                    AbstractAnnotatableTimelineView newTimelineView =
                        new CommunicationsTimelineView(timelineData1);

                    newTimelineView.addTimelineController(this);
                    newTimelineView.setName(data.getName());

                    newTimelineView.addAnnotationMouseListener(
                        new DefaultAnnotationMouseAdapter());
                    newTimelineView.addAnnotationMouseListener(
                        new ColoredMarkerHighlighter());
                    newTimelineView.addMouseListener(new DefaultAnnotatableTimelineMouseAdapter(
                        newTimelineView));

                    event = new CreateTimelineActionEvent(null, data.getName(),
                        data, newTimelineView);
                } else {
                    event = new CreateTimelineActionEvent(null, data.getName(),
                        data);
                }

                requestCreateTimeline(event);
            }

        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());
        } catch (TimelineXMLException xmle) {
            System.err.println(xmle.getMessage());
            xmle.getCause().printStackTrace();
        }
    }

    private void buildTimelineManagementPanel() {
        timelineManagementPanel = new JPanel();
        timelineManagementPanel.setName("Timeline Management Panel");
        timelineManagementPanel.setBackground(Color.DARK_GRAY);

        JButton loadButton = new JButton();
        loadButton.setText("Load");
        loadButton.setSize(100, 80);
        loadButton.setAction(new AbstractAction("Load") {
            @Override
            public void actionPerformed(ActionEvent e) {
                JFileChooser chooser = new JFileChooser();
                int returnVal = chooser.showOpenDialog(timelineManagementPanel);
                if(returnVal == JFileChooser.APPROVE_OPTION) {
                    loadTimelineDataFromFile(chooser.getSelectedFile());
                }
            }
        });

        JButton saveButton = new JButton();
        saveButton.setAction(new AbstractAction("Save") {
            @Override
            public void actionPerformed(ActionEvent event) {
                JFileChooser chooser = new JFileChooser();
                int returnVal = chooser.showSaveDialog(timelineManagementPanel);
                if(returnVal == JFileChooser.APPROVE_OPTION) {
                    try {
                        File outFile = chooser.getSelectedFile();
                        TimelineFileIO.saveTimelinesToFile(
                            getTimelineData(), outFile);
                    } catch(Exception exception) {
                        System.err.println(exception.getMessage());
                    }
                }
            }
        });

        timelineManagementPanel.add(loadButton);
        timelineManagementPanel.add(saveButton);
    }

    private void buildCurrentTimeControl()
    {
        currentTimeControlPanel = new JPanel();
        currentTimeControlPanel.setName("Current Time Control Panel");
        currentTimeControlPanel.setBackground(Color.DARK_GRAY);

        JLabel hoursLabel = new JLabel("Hours");
        hoursLabel.setForeground(Color.WHITE);
        final JTextField hoursField = new JTextField(2);
        hoursField.setText("0");


        JLabel minutesLabel = new JLabel("Minutes");
        minutesLabel.setForeground(Color.WHITE);
        final JTextField minutesField = new JTextField(2);
        minutesField.setText("0");

        JLabel secondsLabel = new JLabel("Seconds");
        secondsLabel.setForeground(Color.WHITE);
        final JTextField secondsField = new JTextField(2);
        secondsField.setText("0");

        JButton setTimeButton = new JButton();
        setTimeButton.setAction(new AbstractAction("Set Current Time") {
            @Override
            public void actionPerformed(ActionEvent e) {
                long currentTime = DefaultTimeline.convertTime(
                    Integer.parseInt(hoursField.getText().trim()),
                    Integer.parseInt(minutesField.getText().trim()),
                    Integer.parseInt(secondsField.getText().trim()));
                getTimelineView().setCurrentTime(currentTime);
            }
        });

        JLabel endTimeLabel = new JLabel("End Time (ms)");
        endTimeLabel.setForeground(Color.WHITE);
        final JTextField endTimeField = new JTextField(6);
        endTimeField.setText("0");

        JButton doubleTimeButton = new JButton();
        doubleTimeButton.setAction(new AbstractAction("Set End Time") {
            @Override
            public void actionPerformed(ActionEvent e) {
                Long timespan = Long.parseLong(endTimeField.getText().trim());
                long markerInterval = timespan / DEFAULT_NUMBER_OF_TIME_MARKERS;
                getTimelineView().setMarkerInterval(markerInterval);
                getTimelineView().setVisibleTimespanEnd(Long.parseLong(
                    endTimeField.getText().trim()));
            }
        });

        JButton addNewTimelineButton = new JButton();
        addNewTimelineButton.setAction(new AbstractAction("Add Timeline") {
            @Override
            public void actionPerformed(ActionEvent e) {
                System.err.println("Add Timeline NOW");
                DefaultAnnotatableTimeline newTimeline = new DefaultAnnotatableTimeline(
                    "heehaw", getTimelineView().getVisibleTimespanEnd());
                CreateTimelineActionEvent event = new CreateTimelineActionEvent(
                    null, "heehaw", newTimeline);
                requestCreateTimeline(event);
                //getTimelineView().setPreferredSize(new Dimension(1200, 800));
                //getTimelineView().setSize(new Dimension(1000, 700));
                /*
                 * let's try to remove all of the timelines (saving them somewhere)
                 * add them back in
                 * and recreate all of the events ...
                 */
                /* RBBEventSpecDialog editor =
                new RBBEventSpecDialog(rbb,getTimelineView().getCurrentTime());
                editor.setUndecorated(true);
                editor.pack();
                editor.setLocation(500, 500);
                editor.setVisible(true); */
            }
        });

        JButton addRBBEventButton = new JButton();
        addRBBEventButton.setAction(new AbstractAction("Add RBB Event") {
            @Override
            public void actionPerformed(ActionEvent e) {
                System.err.println("Add RBB Event NOW");
                RBBEventSpecDialog editor =
                    new RBBEventSpecDialog(rbb,
                    getTimelineView().getCurrentTime());
                editor.setUndecorated(true);
                editor.pack();
                editor.setLocation(500, 500);
                editor.setVisible(true);
                /*for(String name : dataManager.getTimelineNames()) {
                    timelinesToUpdate.add(name);
            }
                updateFromRBB();*/
            }
        });

        JButton setTimelineTagsetButton = new JButton();
        setTimelineTagsetButton.setAction(new AbstractAction(
            "Set Timeline Tagset")
        {

            @Override
            public void actionPerformed(ActionEvent e)
            {
                BufferedReader cin = new BufferedReader(new InputStreamReader(
                    System.in));

                try
                {
                    System.out.print("Timeline number: ");
                    String idString = cin.readLine();
                    Integer timelineNumber = Integer.parseInt(idString) - 1;
                    System.out.print("Tagset string: ");
                    String tagsetString = cin.readLine();

                    /*if(timelineNumber >= 0 && timelineNumber < timelineTagsets.size()) {
                        timelineTagsets.get(timelineNumber).fromString(tagsetString);
                    } else {
                        System.err.println("Invalid tagset number.");
                    }*/
                    System.err.println("No longer implemented.");
                }
                catch (Exception except)
                {
                    System.err.println(except.toString());
                    System.exit(-1);
                }
                updateFromRBB();
            }
        });

        /*currentTimeControlPanel.add(hoursLabel);
        currentTimeControlPanel.add(hoursField);
        currentTimeControlPanel.add(minutesLabel);
        currentTimeControlPanel.add(minutesField);
        currentTimeControlPanel.add(secondsLabel);
        currentTimeControlPanel.add(secondsField);
        currentTimeControlPanel.add(setTimeButton);*/

        currentTimeControlPanel.add(endTimeLabel);
        currentTimeControlPanel.add(endTimeField);
        currentTimeControlPanel.add(doubleTimeButton);
        currentTimeControlPanel.add(addRBBEventButton);
        //currentTimeControlPanel.add(addNewTimelineButton);
        currentTimeControlPanel.add(setTimelineTagsetButton);
    }

    private void buildCompoundTimelinePanel()
    {
        this.getTimelineView().setName("Marked Interval Timeline Panel");
        this.getTimelineView().setPreferredSize(new Dimension(1200, 400));
        this.getTimelineView().setSize(new Dimension(1000, 300));
    }

    private void createAndShowGUI() {
        JFrame f = new JFrame("RBB Event Timeline");
        f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        f.setSize(1500, 800);
        f.setName("The Frame");

        JPanel contentPane = new JPanel();
        contentPane.setName("Content Panel");
        contentPane.setLayout(new BoxLayout(contentPane, BoxLayout.Y_AXIS));
        contentPane.setBackground(Color.DARK_GRAY);

        buildTimelineManagementPanel();
        buildCurrentTimeControl();
        buildCompoundTimelinePanel();

        //contentPane.add(timelineManagementPanel);
        contentPane.add(currentTimeControlPanel);
        contentPane.add(createCompositePanelWithLabels());

        rebuildLabelPanel();

        f.setContentPane(contentPane);
        f.pack();
        f.setLocationRelativeTo(null);
        f.setVisible(true);
    }

    public JPanel createCompositePanelWithLabels() {
        JPanel pnlLabels = new JPanel();
        BoxLayout bl = new BoxLayout(pnlLabels, BoxLayout.Y_AXIS);
        pnlLabels.setLayout(bl);
        pnlLabelsInner = new JPanel(new GridLayout(1, 1));
        pnlLabelsInner.setOpaque(false);
        pnlLabels.add(Box.createRigidArea(new Dimension(-1, 20)));
        pnlLabels.add(pnlLabelsInner);
        pnlLabels.add(Box.createRigidArea(new Dimension(-1, 10)));
        pnlLabels.setBackground(getTimelineView().getBackground());
        pnlLabels.setBorder(BorderFactory.createEmptyBorder(0, 10, 0, 0));
        JPanel pnlTL = new JPanel(new BorderLayout());
        pnlTL.add(this.getTimelineView(), BorderLayout.CENTER);
        pnlTL.add(pnlLabels, BorderLayout.WEST);
        rebuildLabelPanel();
        return pnlTL;
    }

    /*
     * 
     */
    protected DefaultAnnotation eventToAnnotation(Event event) {
        try
        {
            long startTime = (long) (event.getStart() * 1000.0);
            long duration = ((long) (event.getEnd() * 1000.0)) - startTime;
            Tagset tags = event.getTagset();
            String author;

            if ((tags.getValues("author") == null)
                || tags.getValues("author").isEmpty())
            {
                //System.err.format("author was empty%n");
                author = NOTAGVALUE;
            }
            else
            {
                //System.err.println("was not empty");
                author = tags.getValue("author");
                if (author == null)
                {
                    //System.err.println("but was null");
                }
                else
                {
                    //System.err.format("got author /%s/%n", author);
                }
                tags.remove("author");
            }

            String text;
            //System.err.println("about to test text");
            if ((tags.getValues("text") == null)
                || tags.getValues("text").isEmpty())
            {
                //System.err.format("text was empty%n");
                text = NOTAGVALUE;
            }
            else
            {
                //System.err.println("was not empty");
                text = tags.getValue("text");
                if (text == null)
                {
                    //System.err.println("but was null");
                }
                else
                {
                    //System.err.format("got text /%s/%n", text);
                }
                tags.remove("text");
            }

            String type = tags.toString();
            return new DefaultAnnotation(startTime, duration, type, text, author);
        }
        catch (Exception e)
        {
            System.err.println(e.toString());
            return null;
        }
    }

    protected class InfoForAnEvent {
        Double startTime;
        Double endTime;
        Tagset tags;

        InfoForAnEvent() {
            tags = new Tagset();
        }
    }

    protected InfoForAnEvent annotationToEventInfo(Annotation annotation)
    {
        //System.err.format("Hi, I've got an annotation with start %d%n", annotation.getTime());
        InfoForAnEvent eventInfo = new InfoForAnEvent();
        eventInfo.startTime = ((double) annotation.getTime()) / 1000.0;
        eventInfo.endTime = eventInfo.startTime + (((double) annotation.getDuration())
            / 1000.0);
        //System.err.format("Hi, trying to find tags for Type /%s/%n", annotation.getType());
        try
        {
            eventInfo.tags = new Tagset(annotation.getType());
        }
        catch (Exception e)
        {
            System.err.println(e.getMessage());
        }
        if (!NOTAGVALUE.equals(annotation.getText()))
        {
            //System.err.format("adding text /%s/%n", annotation.getText());
            eventInfo.tags.add("text", annotation.getText());
        }
        if (!NOTAGVALUE.equals(annotation.getAuthor()))
        {
            //System.err.format("adding author /%s/%n", annotation.getText());
            eventInfo.tags.add("author", annotation.getAuthor());
        }
        return eventInfo;
    }

    @Override
    public boolean requestDeleteAnnotation(DeleteAnnotationActionEvent event)
    {
        System.err.println("RBBEventTimeline requestUpdateAnnotation");
        Timeline timeline = requestRetrieveTimeline(event.getTimelineTag());
        if (timeline instanceof AnnotatableTimeline)
        {
            AnnotatableTimeline aTimeline = (AnnotatableTimeline) timeline;
            Annotation annotation = aTimeline.getAnnotation(
                event.getDeletedAnnotation().getId());
            InfoForAnEvent originalEvent = annotationToEventInfo(annotation);
            try
            {
                Event[] foundEvents = Event.find(rbb.db(), byTags(originalEvent.tags),
                    byTime(originalEvent.startTime, originalEvent.endTime), withTimeCoordinate(timeCoordinate));
                if (foundEvents.length == 1)
                {
                    H2SEvent.deleteByID(rbb.db(), foundEvents[0].getID());
                    return true;
                }
                else
                {
                    System.err.println(
                        "ERROR: Too many events matching designated annotation!");
                    return false;
                }
            }
            catch (Exception exception)
            {
                System.err.println(exception.getMessage());
                return false;
            }
        }
        else
        {
            return false;
        }
    }

    @Override
    public boolean requestUpdateAnnotation(UpdateAnnotationActionEvent event)
    {
        System.err.println("RBBEventTimeline requestUpdateAnnotation");
        Timeline timeline = requestRetrieveTimeline(event.getTimelineTag());
        if (timeline instanceof AnnotatableTimeline)
        {
            AnnotatableTimeline aTimeline = (AnnotatableTimeline) timeline;
            Annotation annotation = aTimeline.getAnnotation(
                event.getExistingAnnotation().getId());
            InfoForAnEvent originalEvent = annotationToEventInfo(annotation);
            try
            {
                Event[] foundEvents = Event.find(rbb.db(), byTags(originalEvent.tags),
                    byTime(originalEvent.startTime, originalEvent.endTime), withTimeCoordinate(timeCoordinate));
                if (foundEvents.length > 1)
                {
                    System.err.println(
                        "ERROR: Too many events matching designated annotation!");
                    return false;
                }
                double possiblyNewStartTime = ((double) event.getExistingAnnotation().getTime())
                    / 1000.0;
                double possiblyNewEndTime = possiblyNewStartTime
                    + (((double) event.getExistingAnnotation().getDuration())
                    / 1000.0);
                Tagset possiblyNewTags = new Tagset(
                    event.getExistingAnnotation().getType());
                possiblyNewTags.add("text",
                    event.getExistingAnnotation().getText());
                possiblyNewTags.add("author",
                    event.getExistingAnnotation().getAuthor());
                if (foundEvents.length == 1)
                {
                    System.err.format(
                        "updating event start time to %f, end time to %f, and tags to /%s/%n",
                        possiblyNewStartTime, possiblyNewEndTime,
                        possiblyNewTags.toString());
                    H2SEvent.setByID(rbb.db(), foundEvents[0].getID(),
                        possiblyNewStartTime,
                        possiblyNewEndTime, possiblyNewTags.toArray());
                }
                else
                {
                    System.err.println(
                        "Warning:  Creating new event to match this annotation");
                    new Event(rbb.db(), possiblyNewStartTime, possiblyNewEndTime,
                        possiblyNewTags);
                }
                String testValue = possiblyNewTags.getValue("test");
                if (testValue == null || !testValue.equals("draw"))
                {
                    System.err.println("no test=draw, updating");
                    addTimelinesToUpdate();
                    updateFromRBB();
                }
                else
                {
                    System.err.println("test=draw, so no update");
                }
            }
            catch (Exception exception)
            {
                System.err.println(exception.getMessage());
                return false;
            }
        }
        else
        {
            System.err.println("ERROR:  Couldn't find annotation to update!");
            return false;
        }

        return true;
    }

    private void checkForMultiTimelineTagset(Tagset eventTagset) {
        for(Tagset patternTagset : dataManager.getMultiTagsetPatterns()) {
            if(patternTagset.isSubsetOf(eventTagset)) {
                if(!dataManager.alreadyMultiTagsetMatch(eventTagset)) {
                    Tagset matchTagset = new Tagset();
                    for(String name : patternTagset.getNames()) {
                        matchTagset.add(name, eventTagset.getValue(name));
                    }
                    String name = buildMultiTimelineTagsetName(patternTagset, matchTagset);
                    
                    dataManager.addMultiTagsetMatch(matchTagset);
                    dataManager.addSingleTagset(name, matchTagset);
                    dataManager.setTimeline(name, buildTimeline(name));
                }
            }
        }
    }

    private void addTimelinesToUpdate() {
        addTimelinesToUpdate(null);
    }
    private synchronized void addTimelinesToUpdate(Tagset eventTagset) {
        if(eventTagset != null) {
            timelinesToUpdate.addAll(dataManager.appliesToTimelines(eventTagset));
        } else {
            timelinesToUpdate.addAll(dataManager.getTimelineNames());
        }
        
        if(!timelinesToUpdate.isEmpty()) {
            delayedUpdateTimer.restart();
        }
    }
    
    public synchronized void updateFromSpecificEvents(Event[] events, String fieldNameForTimeline) {

        for(String name : dataManager.getTimelineNames()) {
            dataManager.getTimeline(name).clearAnnotations();
        }

        try {
            for(Event event : events) {
                DefaultAnnotation anno = eventToAnnotation(event);

                // Can be null if the event is deleted while we're working.
                if(anno != null) {
                    super.requestCreateAnnotation(
                        new CreateAnnotationActionEvent(event.getTagset().getValue(
                            fieldNameForTimeline), anno));
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
        }

        getTimelineView().updateUI();

        timelinesToUpdate.clear();
        delayedUpdateTimer.stop();
    }

    public MarkedIntervalTimelinePanel getTimelinePanel() {
        return getTimelineView();
    }

    public List<Event> allEvents = new ArrayList<Event>();

    public void invalidateAllTimelinesAndUpdate() {
        for(String name : dataManager.getTimelineNames()) {
            timelinesToUpdate.add(name);
        }
        updateFromRBB();
    }

    public synchronized void updateFromRBB() {

        for(String name : timelinesToUpdate) {
            dataManager.getTimeline(name).clearAnnotations();
        }

//        System.out.println("UPDATE!   with: " + timelinesToUpdate);
        
        allEvents.clear();
        try {
            for(String name : timelinesToUpdate) {
                Event[] events = Event.find(rbb.db(), byTags(dataManager.getTagset(name)), withTimeCoordinate(timeCoordinate));
                for(Event event : events) {
                    allEvents.add(event);
                    DefaultAnnotation anno = eventToAnnotation(event);
                    
                    // Can be null if the event is deleted while we're working.
                    if(anno != null) {
                        super.requestCreateAnnotation(
                            new CreateAnnotationActionEvent(name, anno));

                        String eventColor = event.getTagset().getValue("color");
                        if(eventColor != null) {
                            anno.setType(eventColor);
                        }
                    }
                }
            }
/*            for (int i = 0; i < timelineTagsets.size(); i++) {
                for(Event event : Event.find(rbb.db(), timelineTagsets.get(i), null, null, null, null)) {
                    DefaultAnnotation anno = eventToAnnotation(event);
                    
                    // can be null if the event is deleted while we're working.
                    if(anno != null) {
                        super.requestCreateAnnotation(new CreateAnnotationActionEvent(
                           timelineData.get(i).getName(), anno));
                    }
                }
            }*/
        } catch(Exception e) {
            System.err.println(e.toString());
            System.exit(-1);
        }
        
        getTimelineView().updateUI();
        timelinesToUpdate.clear();
        delayedUpdateTimer.stop();
    }
    
    private DefaultAnnotatableTimeline buildTimeline(String tName) {
        DefaultAnnotatableTimeline newTimeline = 
            new DefaultAnnotatableTimeline(tName, getTimelineView().getVisibleTimespanEnd());
        CreateTimelineActionEvent event = new CreateTimelineActionEvent(null, tName, newTimeline);
        requestCreateTimeline(event);
        return newTimeline;
    }
    
    private String buildMultiTimelineTagsetName(Tagset patternTagset, Tagset matchTagset) {
        String name = "";
        for(String key : patternTagset.getNames()) {
            if(patternTagset.getValue(key) == null) {
                name += matchTagset.getValue(key) + ", ";
            }
        }
        return name.substring(0, name.length() - 2);
    }

    private void buildDefaultTimelineData() {
        
        for(Tagset patternTagset : dataManager.getMultiTagsetPatterns()) {
            try {
                for(Tagset matchTagset : Event.findTagCombinations(rbb.db(), patternTagset.toString())) {
                    String tName = buildMultiTimelineTagsetName(patternTagset, matchTagset);
                    //timelineNames.add(tName);
                    //timelineTagsets.add(matchTagset);
                    dataManager.addSingleTagset(tName, matchTagset);
                    dataManager.addMultiTagsetMatch(matchTagset);
                }
            } catch (SQLException ex) {
                Logger.getLogger(RBBEventTimeline.class.getName()).log(Level.SEVERE,
                    null, ex);
            }
        }
        
        for(String tName : dataManager.getTimelineNames()) {
            DefaultAnnotatableTimeline timeline = buildTimeline(tName);
            dataManager.setTimeline(tName, timeline);
        }
        
        addTimelinesToUpdate();
        updateFromRBB();

        try {
            RBBEventListener.Adapter changeListener = new RBBEventListener.Adapter() {
                @Override public void eventDataAdded(RBB rbb, RBBEventChange.DataAdded ec) { }; // we're not displaying attached data such as timeseries data.
                @Override public void eventChanged(RBB rbb, RBBEventChange change)
                {
                    Tagset eventTagset = null;
                    try { eventTagset = change.event.getTagset(); } catch(Exception e) {}
                    checkForMultiTimelineTagset(eventTagset);
                    addTimelinesToUpdate(eventTagset);
                }
            };
            // todo: empty filterTags means we get ALL event updates, which is abysmal
            rbb.addEventListener(changeListener, new RBBFilter());

            new RBBReplayControl(rbb, new RBBReplayControl.Listener() {

                @Override
                public void replayControl(long simTime, double playRate) {
                    getTimelineView().setCurrentTime(simTime);
                }
            }, 200);

        } catch(Exception e) {
            System.err.format("Exception starting RBBReplayControlTransport: %s%n",
                e.toString());
        }
    }

    public final static String usage =
            "RBBEventTimeline [options...] <rbb_url>]\n"+
            "   options:\n"+
            "   -endTime <60000>: specify maximum of time scale in milliseconds.\n"+
            "   -timeline <DisplayName> <tagset>: Add a timeline with an arbitrary display name, which shows all RBB Events matching the tagset\n"+
            "   -timeCoordinate <timeCoordinate=<name>[,tag2=value2...]: Retrieve and display events in the specified time coordinate.\n"+
            "   -multiTimeline <tagset>: Add a timeline for each unique combination of values for tagnames without values.\n"+
            "       E.g. hour,dayOfWeek,name=Joe will only show events with name=Joe, but split them onto different timelines for each hour of each dayOfWeek.\n"+
            "       E.g. type=AEMASE_flag,MLModel will show all AEMASE flags, with the flags for each model on a separate line.\n";


    public RBBEventTimeline(String args[])
    {
        long markerInterval = 1000;
        Long timespan = 60000L;

        try
        {
            int iArgs = 0;
            for (; iArgs < args.length
                && args[iArgs].substring(0, 1).equals("-"); ++iArgs)
            {
                if (args[iArgs].equalsIgnoreCase("-timeline") ||
                        args[iArgs].equalsIgnoreCase("-timelineTagset")) // deprecated option
                {
                    ++iArgs;
                    if(iArgs >= args.length) {
                        throw new Exception(usage);
                    }
                    String timelineName = args[iArgs];
                    ++iArgs;
                    if(iArgs >= args.length) {
                        throw new Exception(usage);
                    }

                    //timelineNames.add(timelineName);
                    //timelineTagsets.add(new Tagset(args[iArgs]));
                    dataManager.addSingleTagset(timelineName, new Tagset(args[iArgs]));
                }
                else if(args[iArgs].equalsIgnoreCase("-timeCoordinate")) {
                    ++iArgs;
                    if (iArgs >= args.length)
                        throw new Exception(usage);
                    timeCoordinate = new Tagset(args[iArgs]);
                }
                else if (args[iArgs].equalsIgnoreCase("-endTime")) {
                    ++iArgs;
                    if (iArgs >= args.length)
                        throw new Exception(usage);

                    timespan = Long.parseLong(args[iArgs]);
                    markerInterval = timespan / DEFAULT_NUMBER_OF_TIME_MARKERS;
                    //getTimelineView().setMarkerInterval(markerInterval);
                    // getTimelineView().setVisibleTimespanEnd(timespan);
                }
                else if (args[iArgs].equalsIgnoreCase("-multiTimeline") ||
                        args[iArgs].equalsIgnoreCase("-multiTimelineTagset")) {
                    ++iArgs;
                    if (iArgs >= args.length)
                        throw new Exception(usage);
                    
                    Tagset multiTagsetPattern = new Tagset(args[iArgs]);
                    boolean found = false;
                    for(String name : multiTagsetPattern.getNames()) {
                        if(multiTagsetPattern.getValue(name) == null) {
                            found = true;
                        }
                    }
                    if(!found) {
                        System.err.println("Multi-timeline tagset does not have at least one null value.  Consider -timelineTagset instead.");
                    } else {
                        dataManager.addMultiTagsetPattern(multiTagsetPattern);
                    }
                } else {
                    System.err.println("Unrecognized argument " + args[iArgs]);
                }
            }

            if(iArgs != args.length-1) {
                System.err.println("Error: last arg must be rbbURL:\n"+usage);
                System.exit(-1);
            }

            rbb = RBB.connect(args[iArgs]);
            
        } catch(Exception e) {
            System.err.println(e.toString());
            System.exit(-1);
        }

        /*
         * Customize timeline display
         */
        MarkedIntervalTimelinePanel mitp = this.getTimelineView();
        mitp.setVisibleTimespanEnd(timespan);
        mitp.setMarkerInterval(markerInterval);

        /*
         * Populate timeline
         */
        buildDefaultTimelineData();
    }

    private Tagset timeCoordinate=null;

    public static void main(final String args[]) {
        if(args.length==0) {
            System.err.println(usage);
            System.exit(-1);
        }

        EventQueue.invokeLater(new Runnable() {
            @Override
            public void run() {
                RBBEventTimeline frame = new RBBEventTimeline(args);
                frame.createAndShowGUI();
            }
        });
    }

    private class TimelineDataManager {
        private Map<String, Tagset> singleNamedTagsets = new LinkedHashMap<String, Tagset>();
        private Map<String, DefaultAnnotatableTimeline> singleNamedTimelines = new LinkedHashMap<String, DefaultAnnotatableTimeline>();
        
        // The patterns for multi timeline tagsets provided on the command line.
        // For example:
        //     -multiTimelineTagset type=AEMASE_flag,MLModel
        // means this list holds the tagset corresponding to:
        //     type=AEMASE_flag,MLModel 
        private List<Tagset> multiTagsetPatterns = new ArrayList<Tagset>();
        
        // The combinations of tagsets that match the provided patterns.  Kept
        // in memory so as not to duplicate any timelines when dynamically building
        // new timelines from the events.
        // For example:
        //     -multiTimelineTagset type=AEMASE_flag,MLModel
        // means this list holds all the tagsets that match any provided pattern
        // such as:
        //     type=AEMASE_flag,MLModel=near
        //     type=AEMASE_flag,MLModel=far
        //     type=AEMASE_flag,MLModel=test
        //     type=AEMASE_flag,MLModel=blah
        // Each of the permutationIsSubset should be given their own timeline
        private List<Tagset> multiTagsetMatches = new ArrayList<Tagset>();
        
        public void addSingleTagset(String timelineName, Tagset timelineTagset) {
            singleNamedTagsets.put(timelineName, timelineTagset);
        }
        public void addMultiTagsetPattern(Tagset multiTagsetPattern) {
            multiTagsetPatterns.add(multiTagsetPattern);
        }
        public void addMultiTagsetMatch(Tagset multiTagsetMatch) {
            multiTagsetMatches.add(multiTagsetMatch);
        }
        public void setTimeline(String timelineName, DefaultAnnotatableTimeline timeline) {
            singleNamedTimelines.put(timelineName, timeline);
        }
        
        public List<Tagset> getMultiTagsetPatterns() {
            return multiTagsetPatterns;
        }
        public List<Tagset> getMultiTagsetMatches() {
            return multiTagsetMatches;
        }
        public Set<String> getTimelineNames() {
            return singleNamedTagsets.keySet();
        }
        public Tagset getTagset(String name) {
            return singleNamedTagsets.get(name);
        }
        public DefaultAnnotatableTimeline getTimeline(String name) {
            return singleNamedTimelines.get(name);
        }
        
        public boolean alreadyMultiTagsetMatch(Tagset eventTagset) {
            boolean found = false;
            for(Tagset tagsetMatch : multiTagsetMatches) {
                if(tagsetMatch.isSubsetOf(eventTagset)) {
                    found = true;
                }
            }
            return found;
        }
        
        public Set<String> appliesToTimelines(Tagset eventTagset) {
            Set<String> appliesTo = new LinkedHashSet<String>();
            for(String name : singleNamedTagsets.keySet()) {
                if(singleNamedTagsets.get(name).isSubsetOf(eventTagset)) {
                    appliesTo.add(name);
                }
            }
            return appliesTo;
        }
    }
}
