
package gov.sandia.rbb.tools;

import gov.sandia.rbb.Event;
import gov.sandia.rbb.RBB;
import gov.sandia.rbb.Tagset;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.ArrayList;
import static gov.sandia.rbb.RBBFilter.*;

/**
 * Print RBB events as prolog assertions.
 * These can be output to a file and read in by prolog or Jess.
 * This is a way to experiment with reasoning about RBB events in prolog without a compilation dependency.
 * 
 * @author rgabbot
 */
public class GetProlog
{
    public static void main(String[] args)
    {
        try
        {
            prolog(args, System.out);
        }
        catch (Exception e)
        {
            System.err.println("Got Exception: " + e.getMessage());
            // e.printStackTrace();
        }
    }

    public static void prolog(String[] args,
        PrintStream out) throws Exception
    {
        String usage =
            "Jess [-start 123.456] [-end 123.456] [-timestep 1.234] [-timeCoordinate name=val...] JDBC_URL name=value,... name=value,... ... ";
        Double start=null, end=null;

        int iArg = 0;
        Tagset timeCoordinate = null;

        for (; iArg < args.length; ++iArg)
        {
            if (args[iArg].equals("-start"))
            {
                ++iArg;
                if (iArg >= args.length)
                {
                    throw new Exception(
                        "-start requires a parameter specifying the start time");
                }
                start = Double.parseDouble(args[iArg]);
            }
            else if (args[iArg].equals("-end"))
            {
                ++iArg;
                if (iArg >= args.length)
                {
                    throw new Exception(
                        "-end requires a parameter specifying the end time");
                }
                end = Double.parseDouble(args[iArg]);
            }
            else if (args[iArg].equals("-timeCoordinate"))
            {
                ++iArg;
                if (iArg >= args.length)
                {
                    throw new Exception(
                        "-timeCoordinate requires a tagset string specifying a time coordinate from RBB_TIME_COORDINATES table, e.g. timeCoordinate=secondsUTC");
                }
                timeCoordinate = new Tagset(args[iArg]);
            }
            else if (args[iArg].charAt(0) == '-')
            {
                throw new Exception("Unrecognized switch " + args[iArg]);
            }
            else
            {
                break;
            }
        }

        if (iArg == args.length)
        {
            throw new Exception(usage + "\nMust specify database URL!");
        }

        RBB rbb = RBB.connect(args[iArg]);
        ++iArg;

        //Rete prolog = new Rete();
        PrintStream ps = System.out;

        // ps.println("(defmodule RBB)");
//        ps.println("(deftemplate evt-start (slot id) (slot time))");
//        ps.println("(deftemplate evt-end (slot id) (slot time))");
//        ps.println("(deftemplate evt-tag (slot id) (slot name) (slot value))");

        ps.println("evt-next(ID1, ID2) :- evt-start(ID1, T1), evt-next-time(T1, T2), evt-start(ID2, T2).");

        ps.println("evt-next(ID1, ID2, 1) :- evt-next(ID1, ID2).");
        ps.println("evt-next(ID1, ID3, N) :- N > 1, M is N-1, evt-next(ID1, ID2), evt-next(ID2, ID3, M).");

        ArrayList<Tagset> tags = new ArrayList<Tagset>();
        for(; iArg < args.length; ++iArg)
            tags.add(new Tagset(args[iArg]));
        if(tags.isEmpty())
            tags.add(null);

//        Set<String> templates = new HashSet<String>();

        for(int i = 0; i < tags.size(); ++i) {
            prevTime = null;
            for(Event evt : Event.find(rbb.db(), byTags(tags.get(i)), byTime(start, end), withTimeCoordinate(timeCoordinate)))
//            for(Event evt : Event.find(rbb.db(), tags.get(i), start, end, timeCoordinate, null))
                makeRuleProlog(System.out, evt);
        }

        rbb.disconnect();

    }

    public static void print(PrintStream ps, Object... strings) {
        for(Object s : strings)
            ps.print(s);
    }

    public static void makeRule(PrintStream ps, Event evt) throws SQLException {
        print(ps, "(assert (evt-start (id ", evt.getID(), ") (time ", evt.getStart(), ")))\n");

        if(evt.getEnd() < java.lang.Double.MAX_VALUE)
            print(ps, "(assert (evt-end (id ", evt.getID(), ") (time ", evt.getEnd(), ")))\n");

        for(String name : evt.getTagset().getNames()) {
            for(String value : evt.getTagset().getValues(name)) {
                print(ps, "(assert (evt-tag (id ", evt.getID(), ") (name ", name, ") (value ", value, ")))\n");
            }
        }
    }

    private static Double prevTime;

   public static void makeRuleProlog(PrintStream ps, Event evt) throws SQLException {
       if(prevTime == null || evt.getStart() != prevTime) {
           if(prevTime != null)
            print(ps, "evt-next-time(", prevTime, ", ", evt.getStart(), ").\n");
           prevTime = evt.getStart();
       }

        print(ps, "evt-start(", evt.getID(), ", ", evt.getStart(), ").\n");

        if(evt.getEnd() < java.lang.Double.MAX_VALUE)
            print(ps, "evt-end(", evt.getID(), ", ", evt.getEnd(), ").\n");

        for(String name : evt.getTagset().getNames()) {
            for(String value : evt.getTagset().getValues(name)) {
                print(ps, "evt-tag(", evt.getID(), ", '", name, "', '", value, "').\n");
            }
        }
    }
}
// java -cp $H2:$RBB RBB.util.Get jdbc:h2:tcp://localhost//tmp/db/db

