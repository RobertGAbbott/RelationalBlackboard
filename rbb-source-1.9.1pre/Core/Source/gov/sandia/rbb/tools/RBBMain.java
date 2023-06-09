/*
 * File:                RBBMain.java
 * Authors:             Justin Basilico
 * Company:             Sandia National Laboratories
 * Project:             Relational Blackboard Core
 * 
 * Copyright September 16, 2010, Sandia Corporation.
 * Under the terms of Contract DE-AC04-94AL85000, there is a non-exclusive 
 * license for use of this work by or on behalf of the U.S. Government. Export 
 * of this program may require a license from the United States Government. 
 */

package gov.sandia.rbb.tools;

import gov.sandia.rbb.RBB;
import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.impl.h2.statics.H2SBlob;
import gov.sandia.rbb.impl.h2.statics.H2SEvent;
import gov.sandia.rbb.impl.h2.H2EventTCPServer;
import gov.sandia.rbb.impl.h2.statics.H2SRBB;
import gov.sandia.rbb.impl.h2.statics.H2SString;
import gov.sandia.rbb.impl.h2.statics.H2STime;
import gov.sandia.rbb.util.StringsWriter;
import java.io.ByteArrayInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.util.Arrays;
import org.h2.tools.Shell;
import static gov.sandia.rbb.RBBFilter.*;

/**
 *
 * This program is a simple frontend to run any of the programs in this package.
 * 
*/
public class RBBMain
{

    /**
     * This is for calling RBBMain from the command line - from java code call RBBMain instead.
     * If there is an exception, it prints out the message and aborts the program with an error code.
     */
    public static void main(String[] args) {
        try
        {
            RBBMain(args);
        }
        catch(Throwable e)
        {
            System.err.println(e.toString());
            System.exit(-1);
        }
    }

    /**
     * unlike main(), RBBMain() propagates exceptions so it is more useful for
     * calling from other code or unit tests.
     *
     * To call it from other code you will typically:
     * import static gov.sandia.rbb.RBBMain.RBBMain;
     *
     */
    public static void RBBMain(String... args) throws Throwable
    {
        if (args == null || args.length <= 0)
        {
            // NOTE! the 1-liner for any command that has subcommands should use the word "subcommand" in it.  This is assumed when generating documentation.
            System.err.println(
                "Usage: RBBMain <command> [args]");
            System.err.println("Commands:  (invoke a command without arguments for more detail)");
            System.err.println("    attachBlob - Attach arbitrary data to an RBB Event.");
            System.err.println("    countEventTags - list the values that occur for each individual tag name, and the number of uses.");
            System.err.println("    create - Create an RBB DB.");
            System.err.println("    get - Retrieve timeseries from an RBB, optionally interpolating to create snapshots of co-occuring data.");
            System.err.println("    getBlob - Retrieve arbitrary data previously attached to an RBB Event");
            System.err.println("    getTagsets - retrieve all combinations of values that co-occur for specified tag names in Event Tagsets.");
            System.err.println("    put - Put data into an RBB DB.");
            System.err.println("    defineTimeCoordinate - Create a time coordinate.");
            System.err.println("    defineTimeCoordinatesForEventCombinations - Create time coordinate for each combination of tag values.");
            System.err.println("    delete - (Permanently) delete events from the RBB.");
            System.err.println("    deleteAttachments - delete data attached to an RBB event, e.g. with attachBlob");
            System.err.println("    deleteRBB - delete the entire RBB and the file(s) it was stored in.");
            System.err.println("    findEventSequences - find patterns in time-ordered events.");
            System.err.println("    ml <subcommand> - RBB ML (machine learning) (rbbml.jar must be in classpath)");
            System.err.println("    server - Runs the H2 server with RBB extensions available (i.e. in the classpath), and passing any arguments to the H2 server.");
            System.err.println("    shell - run H2 shell (CLI console) on an RBB");
            System.err.println("    stats - print out statistics about the size of the RBB.");
            System.err.println("    setTags - add or change tags to events matching the filterTags or ID");
            System.err.println("    tags - create the string representation of an RBB Tagset from args name1 value1 name2 value2...");
            System.err.println("    TCPServer - print the hostname and port number of the Event server, starting it if necessary.");
            System.err.println("    removeTags - remove any/all values of the specified tags for matching Events.");
            System.err.println("    ui <subcommand> - graphical display and interaction with an RBB (rbbui.jar must be in classpath)");
            System.err.println("    version - print the version of this RBB library: <major>.<DB Schema Version>.<revision>");
            // updateSchema not unviersally implemented... feel free to try it though.
            // System.err.println("    updateSchema <dbURL>: update an RBB to current schema version (warning: modifies the RBB)");
            return;
        }

        final String cmd = args[0];
        args = Arrays.copyOfRange(args, 1, args.length);

        if (cmd.equalsIgnoreCase("create"))
        {
            String url = null;
            String name = null;

            if(args.length == 1)
                url = args[0];
            else if(args.length == 2) {
                url = args[0];
                name = args[1];
            }
            else {
                throw new Exception("Usage: create <db_url> [human_readable_name] - creates a new RBB, or populates existing database with new RBB tables.");
            }
            RBB rbb = RBB.create(url, name);
        }
        else if (cmd.equalsIgnoreCase("get"))
        {
            Get.get(System.out, args);
        }
        else if (cmd.equalsIgnoreCase("jess"))
        {
            GetProlog.main(args);
        }
        else if (cmd.equalsIgnoreCase("put"))
        {
            Put.main(args);
        }
        else if (cmd.equalsIgnoreCase("delete"))
        {
            String usage = "Usage: DeleteEvents <JDBC_URL> <-id N>|<filterTags>";
            if(args.length < 2)
                throw new Exception(usage);
            RBB rbb = RBB.connect(args[0]);
            int n;
            if(args.length == 2)
                n = H2SEvent.delete(rbb.db(), byTags(args[1]));
            else if(args.length == 3) {
                if(!args[1].equalsIgnoreCase("-id"))
                    throw new Exception(usage);
                n = H2SEvent.deleteByID(rbb.db(), Long.parseLong(args[2]));
            }
            else
                throw new Exception(usage);
            rbb.disconnect();
            System.err.println("Deleted " + n + " events");
        }
        else if (cmd.equalsIgnoreCase("deleteAttachments"))
        {
            H2SEvent.deleteDataCLI(args);
        }
        else if (cmd.equalsIgnoreCase("server"))
        {
            org.h2.tools.Server.main(args);
        }
        else if (cmd.equalsIgnoreCase("deleteRBB"))
        {
            RBB rbb = RBB.connect(args[0]);
            rbb.deleteRBB();
        }
        else if (cmd.equalsIgnoreCase("findEventSequences"))
        {
            FindEventSequences.main(args);
        }
        else if (cmd.equalsIgnoreCase("setTags"))
        {
            String usage = "Usage: setTags <RBB_URL> <filterTags|-id ID> <setTags>";
            if(args.length < 3)
                throw new Exception(usage);
            RBB rbb = RBB.connect(args[0]);
            if(args.length==3) {
                H2SEvent.setTags(rbb.db(), args[1], args[2]);
            }
            else if(args.length==4) {
                if(!args[1].equalsIgnoreCase("-id"))
                    throw new Exception(usage);
                H2SEvent.setTagsByID(rbb.db(), Long.parseLong(args[2]), args[3]);
            }
            else
                throw new Exception(usage);
            rbb.disconnect();
        }
        else if (cmd.equalsIgnoreCase("removeTags"))
        {
            H2SEvent.removeTagsCLI(args);
        }
        else if (cmd.equalsIgnoreCase("shell"))
        {
            int iArg=0;
            String ifExists = ";ifexists=true";
            if(args.length > 0 && args[iArg].equalsIgnoreCase("-create")) {
                ++iArg;
                ifExists = "";
            }

            if(iArg >= args.length)
                throw new Exception("usage: shell [-create] <JDBC_URL>");
            String[] shellArgs = new String[] {"-user", "sa", "-password", "x", "-url", args[iArg]+ifExists };
            ++iArg;
            Shell shell = new Shell();
            if(iArg  < args.length-1)
                shell.setIn(new ByteArrayInputStream(StringsWriter.join("", Arrays.copyOfRange(args, iArg, args.length)).getBytes()));
            shell.runTool(shellArgs);
        }
        else if (cmd.equalsIgnoreCase("stats"))
        {
            if(args.length != 1) {
                throw new Exception("Usage: stats <rbbURL>");
            }
            RBB rbb = RBB.connect(args[0]);
            rbb.printStats(System.out);
            rbb.disconnect();
        }
        else if (cmd.equalsIgnoreCase("countEventTags")) {
            countEventTags(args);
        }
        else if (cmd.equalsIgnoreCase("getTagsets")) {
            if(args.length == 0)
                throw new Exception("getTagsets requires a JDBC url");
            RBB rbb = RBB.connect(args[0]);
            SortedTagsets gt = new SortedTagsets(rbb);
            gt.parseArgs(Arrays.copyOfRange(args, 1, args.length));
            gt.print(System.out);
            rbb.disconnect();
        }
        else if (cmd.equalsIgnoreCase("attachBlob")) {
            H2SBlob.attachBlobCLI(args);
         }
        else if (cmd.equalsIgnoreCase("getBlob")) {
            H2SBlob.getBlobCLI(args);
         }
        else if(cmd.equalsIgnoreCase("updateSchema")) {
            UpdateSchema.main(args);
        }
        else if (cmd.equalsIgnoreCase("defineTimeCoordinate")) {
            String usage = "Usage: defineTimeCoordinate <RBB_URL> <timeCoordinate=XXX[,moreTags...]> <slope> <intercept>, e.g. jdbc:h2:mydb timeCoordinate=millisecondsUTC 1000 0.0";
            if(args.length!=4)
                throw new Exception(usage);
            RBB rbb = RBB.connect(args[0]);
            final double slope = Double.parseDouble(args[2]);
            final double intercept = Double.parseDouble(args[3]);
            H2STime.defineCoordinate(rbb.db(), args[1], slope, intercept);
            rbb.disconnect();
        }
        else if (cmd.equalsIgnoreCase("defineTimeCoordinatesForEventCombinations")) {
            String usage = "Usage: defineTimeCoordinatesForEventCombinations <RBB_URL> <coordinateName> <tags> <timeScale>, e.g. jdbc:h2:mydb sessionMilliseconds sessionID 1000";
            if(args.length!=4)
                throw new Exception(usage);
            RBB rbb = RBB.connect(args[0]);
            H2SEvent.defineTimeCoordinatesForEventCombinations(rbb.db(), args[1], args[2], // passing null here does preclude requiring two-different values of the same tag for a time coordinate
                null, Double.parseDouble(args[3]));
            rbb.disconnect();
        }
        else if (cmd.equalsIgnoreCase("ml")) {
            call("gov.sandia.rbb.ml.RBBML", "RBBMLMain", args);
        }
        else if (cmd.equalsIgnoreCase("ui")) {
            call("gov.sandia.rbb.ui.UIMain", "UIMain", args);
        }
        else if (cmd.equalsIgnoreCase("TCPServer")) {
            String usage = "Usage: TCPServer [-stop] jdbc:h2:...";
            if(args.length < 1)
                throw new Exception(usage);
            int arg=0;
            boolean start=true;
            if(args[arg].equalsIgnoreCase("-stop")) {
                ++arg;
                start=false;
            }

            if(arg != args.length-1)
                throw new Exception(usage);
            
            RBB rbb = RBB.connect(args[arg]);

            if(start) {
                System.out.println(H2SRBB.getServerAddress(rbb.db()));
                System.out.println(H2SRBB.startEventTCPServer(rbb.db()));
            }
            else {
                H2EventTCPServer.stop(rbb.db());
            }

        }
        else if(cmd.equalsIgnoreCase("tags")) {
            Tagset t = new Tagset(args);
            System.out.println(t);
        }
        else if (cmd.equalsIgnoreCase("version")) {
            System.out.println("1."+ // if this changes, may have to repeat copyright assertion process for RBB
                    H2SRBB.schemaVersion()+
                    ".1pre" // increment each time software is released to rbb.sandia.gov
                    );
        }
        else
        {
            System.err.println("Unknown command: " + cmd);
        }
    }

    public static void call(String className, String methodName, String[] args) throws Throwable {
        Class<?> classInstance = RBBMain.class.getClassLoader().loadClass(className);
        Method methodInstance = classInstance.getMethod(methodName, new Class[]{args.getClass()});
        try {
            methodInstance.invoke(null, new Object[]{args});
        } catch(InvocationTargetException e) {
            // just pass along the underlying exception as if it had come from a direct call.
            throw e.getCause();
        }
    }

    public static void countEventTags(String[] args) throws Exception {
        if(args.length != 1) {
            throw new Exception("Usage: countEventTags <RBBURL>\n"+
                    "Output: tagname (N): tagValue1 (M1)[, tagValue2 (M2)...] N = number of events with any value for the tag.  M = number of events with this value for this tag.\n"+
                    "Note: A tag name is counted at most once per event (even if the event has several values for the tag).");
        }
        RBB rbb = RBB.connect(args[0]);

        // the reason for returning value_ids and then converting them to strings is because
        // group_concat returns a string, not a true array, so any character used as delimeter might
        // occur in the tag values.
        final String q = "select sum(n) TOTAL, rbb_id_to_string(NAME_ID) NAME, group_concat(n) NVALUES, group_concat(VALUE_ID) VALUE_IDS  from (select NAME_ID, VALUE_ID, rbb_id_to_string(VALUE_ID) VALUE, count(*) as N from RBB_TAGSETS T join RBB_EVENTS E on T.tagset_id = E.tagset_id group by name_id, value_id order BY N desc) group by NAME_ID order by TOTAL desc, NAME;";
        ResultSet rs = rbb.db().createStatement().executeQuery(q);
        while(rs.next()) {
            System.out.print(rs.getString("NAME") + " (" + rs.getInt("TOTAL") + "):");
            String[] values = H2SString.fromIDs(rbb.db(), rs.getString("VALUE_IDS").split(","));
            String[] n = rs.getString("NVALUES").split(",");
            for(int i = 0; i < n.length; ++i)
                System.out.print((i==0?" ":", ")+values[i]+(i < n.length ? " ("+n[i]+")" : ""));
            System.out.println(""); // end the line.
        }

        rbb.disconnect();
    }

}
