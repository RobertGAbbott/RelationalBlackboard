/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ui;

import javax.swing.SwingUtilities;
import gov.sandia.rbb.tools.RBBMain;
import gov.sandia.rbb.RBB;
import gov.sandia.rbb.ui.timeline.RBBEventTimeline;
import java.util.Arrays;
import static gov.sandia.rbb.ui.ImagePanel.ImagePanelMain;

/**
 *
 * This main function allows running independent UI components from the command-line
 *
 * @author rgabbot
 */
public class UIMain {

    /**
     * This is for calling UIMain from the command line - from java code call UIMain instead.
     * If there is an exception, it prints out the message and aborts the program with an error code.
     */
    public static void main(String[] args) {
        try
        {
            UIMain(args);
        }
        catch(Throwable e)
        {
            System.err.println(e.getMessage());
            System.exit(-1);
        }
    }

    /**
     * unlike main(), UIMain() propagates exceptions so it is more useful for
     * calling from other code or unit tests.
     *
     * To call it from other code you will typically:
     * import static gov.sandia.rbb.ml.UIMain.UIMain;
     *
     */
    public static void UIMain(String... args) throws Throwable
    {
        String usage =
                    "UIMain <command> [args...]\n"+
                    "Commands:  (invoke a command without arguments for more detail):\n"+
                    "   draw - interactively view and create RBB Timeseries as 2d trajectories\n"+
                    "   timeline - display Events on a timeline\n"+
                    "   images - display images attached to Events in the 'images' schema.\n"+
                    "   play - replay an audio or video file using MPlayer.  The replay is synchronized with other RBB components.\n"+
                    "   plot - plot a timeseries against time using JFreeChart.\n"+
//                    "   tagTable - a table view of tagset values.\n"+
                    "   tagTree - a tree view of tagset values.\n"+
                    "   timeSlider - a horizontal scrollbar for controlling time in RBB components\n";

        // strip command from args
        if(args.length == 0) {
            throw new Exception(usage);
        }
        final String cmd = args[0];
        args = Arrays.copyOfRange(args, 1, args.length);

        // process the command.
        if(cmd.equalsIgnoreCase("draw")) {
            // This must be done BEFORE creating DrawTimeseries.
            // That loads the DrawTimeseries class, which inherits from JFrame,
            // and setting the name has no effect after that has been done.
            System.setProperty("com.apple.mrj.application.apple.menu.about.name", "RBB Draw");
            DrawTimeseries draw = new DrawTimeseries(args);
        }
        else if(cmd.equalsIgnoreCase("timeline")) {
            RBBEventTimeline.main(args);
        }
        else if(cmd.equalsIgnoreCase("images")) {
            ImagePanelMain(args);
        }
        else if(cmd.equalsIgnoreCase("play")) {
            if(args.length != 2)
                throw new Exception("Usage: play <JDBC_URL> <mediaFilename.xxx>");
            RBB rbb = RBB.connect(args[0]);
            MPlayerReplayClient player = new MPlayerReplayClient(rbb, args[1]);
//            player.waitForMPlayerToExit();
        }
//         else if(cmd.equalsIgnoreCase("tagTable")) {
//            if(args.length==0)
//                throw new Exception("TagTable exception: first arg is RBB JDBC URL");
//            RBB rbb = RBB.connect(args[0]);
//            TagTable.TagTableMain(rbb, Arrays.copyOfRange(args, 1, args.length));
//         }
         else if(cmd.equalsIgnoreCase("tagTree")) {
            if(args.length==0)
                throw new Exception("TagTree error: last arg is RBB JDBC URL");
            final String jdbc = args[args.length-1];
            RBB rbb = RBB.connect(jdbc);
            new TagTree(rbb, Arrays.copyOfRange(args, 0, args.length-1));
         }
        else if(cmd.equalsIgnoreCase("plot")) {
            // do not call directly as this would create a dependency on the JFreeChart jarfile even if plot is never called.
            RBBMain.call("gov.sandia.rbb.ui.chart.TimeseriesXYChart", "main", args);
        }
        else
        {
            System.out.println("Unknown command: " + cmd);
        }

//        System.err.println("UIMain exiting.");
    }
}
