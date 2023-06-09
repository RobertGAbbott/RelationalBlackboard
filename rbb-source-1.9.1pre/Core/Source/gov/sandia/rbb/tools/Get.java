
package gov.sandia.rbb.tools;

import gov.sandia.rbb.Event;
import gov.sandia.rbb.RBB;
import gov.sandia.rbb.RBBFilter;
import gov.sandia.rbb.Tagset;
import static gov.sandia.rbb.RBBFilter.*;
import gov.sandia.rbb.impl.h2.statics.H2SEvent;
import gov.sandia.rbb.impl.h2.statics.H2STimeseries;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Retrieve sets of concurrent events (i.e. events that overlap in time) and that
 * match specified filters for tags and times.
 * Run without args for a help string, and see
 * gov.sandia.rbb.tools.GetTest
 *
 * @author rgabbot
 */
public class Get
{

    /**
     *  "Get [-start 123.456] [-end 123.456] JDBC_URL col1name1=col1val1,col1name1=col1val2,...  col2name1=col2val1,col2name1=col2val2,...";
     * @param args
     */
    public static void main(String[] args)
    {
        try
        {
            get(System.out, args);
        }
        catch (Exception e)
        {
            System.err.println("Got Exception: " + e.getMessage());
            // e.printStackTrace();
        }
    }

    public static void get(PrintStream out, String... args) throws Exception
    {
        String usage =
            "Get [options] JDBC_URL tagset [tagset_2...]\n"+
            "In a tagset:\n"+
            "  <name>=<value>: Only matches tagsets with (at least) that tag name and value\n"+
            "  <name>: This is a null-valued tag; it matches any tagset with that tag name, regardless of value\n"+
            "  <name>=: This is an empty-valued tag; it matches only the same value for this tag as in the tagset to its left (Join)\n"+
            "  start=<1.234>: This is a reserved value which must be specified for Events that are not timeseries\n"+
            "  end=<1.234>: This is a reserved value which must be specified for Events that are not timeseries\n"+
            "  time=<1.234>: This is a reserved value which may be used instead of start/end to specify an instantaneous event\n"+
            "Options:\n"+
            "  -csv: Output tab-delimited format for csv spreadsheet import\n"+
            "  -end 123.456: Disregard events/timeseries that started after this time\n" +
            "  -noTimes: Do not add 'start' and 'end' tags (or a 'time' tag, if start=end) to each output\n" +
            "  -RBBID: Add a tag RBBID=<id> to the reported tagset of each event / timeseries\n"+
            "  -schema <schemaName>: Limit the search to Events with data in the specified schema\n"+
            "  -start 123.456: Disregard events/timeseries that ended before this time\n"+
            "  -timestep 1.234: Resample timeseries with the specified frequency\n"+
            "  -tagsOnly: Output timeseries as events (no Samples)\n"+
            "  -timeCoordinate: Interpret and output times in the specified time coordinate\n";


        int iArg = 0;
        Double start = null, end = null, timestep = null; // store the values of the -start, -end, -timestep options.
        String timeCoordinate = null;
        boolean tagsOnly = false; // if true, only the tagset for each event is output - not the data.
        boolean idTag = false; // if true, an 'id=<n> tag is synthesized for each retrieved event.
        boolean csv = false; // if true, output tab-delimited data (e.g. for spreadsheet import)
        String schema = null;
        boolean printTimes = true;

        for (; iArg < args.length; ++iArg)
        {
            if (args[iArg].equalsIgnoreCase("-start"))
            {
                ++iArg;
                if (iArg >= args.length)
                {
                    throw new Exception(
                        "-start requires a parameter specifying the start time");
                }
                start = Double.parseDouble(args[iArg]);
            }
            else if (args[iArg].equalsIgnoreCase("-end"))
            {
                ++iArg;
                if (iArg >= args.length)
                {
                    throw new Exception(
                        "-end requires a parameter specifying the end time");
                }
                end = Double.parseDouble(args[iArg]);
            }
            else if (args[iArg].equalsIgnoreCase("-timestep"))
            {
                ++iArg;
                if (iArg >= args.length)
                {
                    throw new Exception(
                        "-timestep requires a parameter specifying the timestep");
                }
                timestep = Double.parseDouble(args[iArg]);
            }
            else if (args[iArg].equalsIgnoreCase("-timeCoordinate"))
            {
                ++iArg;
                if (iArg >= args.length)
                {
                    throw new Exception(
                        "-timeCoordinate requires a tagset string specifying a time coordinate from RBB_TIME_COORDINATES table, e.g. timeCoordinate=secondsUTC");
                }
                timeCoordinate = args[iArg];
            }
            else if (args[iArg].equalsIgnoreCase("-schema"))
            {
                ++iArg;
                if (iArg >= args.length)
                    throw new Exception("-schema requires a tagset string specifying a time coordinate from RBB_TIME_COORDINATES table, e.g. timeCoordinate=secondsUTC");
                schema =args[iArg];
            }
            else if (args[iArg].equalsIgnoreCase("-tagsOnly"))
            {
                tagsOnly = true;
            }
            else if (args[iArg].equalsIgnoreCase("-csv"))
            {
                csv = true;
            }
            else if (args[iArg].equalsIgnoreCase("-noTimes"))
            {
                printTimes = false;
            }
            else if (args[iArg].equalsIgnoreCase("-RBBID")) {
                idTag = true;
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

        if (iArg == args.length)
        {
            throw new Exception(
                usage
                + "\nMust specify one or more filter tagsets specifying events to retrieve as columns!");
        }

//        //// TODO:
//        // "An array with one element must contain a comma to be parsed as an array" - http://www.h2database.com/html/grammar.html#array
//        // will this fail if a single "concurrent" timeseries is retrieved?
        final int numCols = args.length-iArg; // the number of data columns to be retrieved.

        RBBFilter[] filters = new RBBFilter[numCols];
        for(int i =  0; i < numCols; ++i)
            filters[i] = RBBFilter.fromString(args[iArg+i]);

        ResultSet rs;
        if(numCols==1) // special case for 1 column - no need to search for concurrent events.
            rs = H2SEvent.find(rbb.db(), filters[0], byTime(start,end), withTimeCoordinate(timeCoordinate), bySchema(schema));
        else
            rs = H2SEvent.findConcurrent(rbb.db(), filters, null, start, end, timeCoordinate, schema);

        // loop over the sets of concurrent events
        while (rs.next())
        {
            final Double start0 = rs.getDouble("START_TIME");
            final Double end0 = rs.getDouble("END_TIME");

            // get an array of Event instances representing the current set of concurrent events.
            Event[] events;
            if(numCols==1)
                events = new Event[]{ new Event(rs) };
            else
            {
                Long[] ids = (Long[]) rs.getArray("IDS").getArray();
                RBBFilter f = byID(ids);
                if(timeCoordinate != null)
                    f.also(withTimeCoordinate(timeCoordinate));
                events = Event.getByIDs(rbb.db(), ids, f);
            }

            // now add any requested extra tags to each event
            for(Event event : events)
            {
                // Change the tagset of the synthesized event to show the new time coordinate.
                // This still doesn't seem complete, since the new time coordinate might only be computable in the context of events to the left;
                // shouldn't those contextual tags show up here?  Although, if used as join tags ('name=') then they will.
                Tagset tags = event.getTagset();
                if(printTimes) {
                    if(start0.equals(end0))
                        tags.add("time", start0.toString());
                    else {
                        tags.add("start", start0.toString());
                        tags.add("end", end0.toString());
                    }
                }
                if(idTag)
                    tags.set("RBBID", event.getID().toString());
            }

            // get the dimension of the timeseries data for each event (null if not a timeseries)
            // dim.size() will be the number of timeseries among the events.
            Map<Long, Integer> timeseriesDim = null;
            if(!tagsOnly) {
                timeseriesDim = new HashMap<Long, Integer>();
                for(Event event : events) {
                    Integer d = H2STimeseries.getDim(rbb.db(), event.getID());
                    if(d != null)
                        timeseriesDim.put(event.getID(), d);
                }
            }

            // now print the events
            for(int i = 0; i < events.length; ++i)
            {
                if(csv)
                    out.print("Time\t");
                out.print(events[i].getTagset());
                if(i == events.length-1)
                    continue; // don't need a delimiter before next tagset.
                if(!csv) {
                    out.print(" "); // normally the delimeter is just a space.
                } else {
                    // in the case of tab-delimiting, we need to align the next tagset over its data by putting out a tab delimeter for
                    int dim = 1; // default for non-timeseries
                    if(!tagsOnly && timeseriesDim.containsKey(events[i].getID()))
                        dim = timeseriesDim.get(events[i].getID());
                    for(int j = 0; j < dim; ++j)
                        out.print("\t");
                }
            }
            out.println(""); // print newline to end the row of events.

            if (!tagsOnly && timeseriesDim.size() > 0)
            {
                // get the timeseries values
                ResultSet rs1 = null;

                // this is a special case where no timestep is specified and there's only one timeseries,
                // so there's no need for resampling - just get the values and times of the timeseries.
                if(events.length == 1 && timestep == null) {
                    rs1 = H2STimeseries.getSamples(rbb.db(), events[0].getID(), start0, end0,
                        0, 0, timeCoordinate, null);
                }
                else {
                    Double[] sampleTimes=null;
                    if(timestep == null) {
                        // no timestep specified = use sample times for first timeseries.
                        // TODO: this path retrieves the first timeseries twice, first to get the times, then in resampleValues to get the values.
                        //   So it could be better optimized.
                        sampleTimes = H2STimeseries.getSampleTimes(rbb.db(), events[0].getID(), start0, end0, timeCoordinate, null);
                    }
                    else {
                        int n = 1+(int)((end0-start0)/timestep);
                        sampleTimes = new Double[n];
                        for(int i = 0; i < n; ++i)
                            sampleTimes[i] = start0 + i * timestep;
                    }
                    rs1 = H2STimeseries.resampleValues(rbb.db(), Event.getIDs(events),
                            sampleTimes, timeCoordinate);
                }

                // print the timeseries values.
                while (rs1.next()) // each row has samples at an instant.
                { // loop over events for this instant.
                    for (int iEv = 0; iEv < events.length; ++iEv)
                    {
                        if (iEv > 0)
                            out.print(csv ? "\t" : " ");
                        out.print(rs1.getDouble("TIME"));

                        if(timeseriesDim.containsKey(events[iEv].getID())) {
                            Object[] data = (Object[]) rs1.getArray(iEv + 2).getArray(); // +2 because GetArray is 1-based, and 1st col is time
                            for (int iDim = 0; iDim < data.length; ++iDim)
                                out.print((csv ? "\t" : ",") + data[iDim].toString());
                        }
                    }
                    out.println(""); // newline after each time step
                }
                out.println(""); // blank line after each set of timeseries
            }
        }

        rbb.disconnect();

    }

}
// java -cp $H2:$RBB RBB.util.Get jdbc:h2:tcp://localhost//tmp/db/db

