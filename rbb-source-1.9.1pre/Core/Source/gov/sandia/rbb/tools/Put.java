
package gov.sandia.rbb.tools;

import gov.sandia.rbb.Event;
import gov.sandia.rbb.RBB;
import gov.sandia.rbb.RBBFilter;
import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.impl.h2.statics.H2SEvent;
import gov.sandia.rbb.impl.h2.statics.H2STimeseries;
import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.SQLException;
import java.util.ArrayList;


/**
 * Input events and timeseries data to an RBB using a simple line-oriented text format (the same format as Get).
 * Run without args for a help string, and see
 * gov.sandia.rbb.tools.PutTest
 *
 *
 * @author rgabbot
 */
public class Put
{
    public static void main(String[] args)
    {
        boolean mux = false; // if true, each line is prefixed with an arbitrary identifier creating separate sequences of lines.
        java.io.BufferedReader input = null; // will be set to input file or stdin if no input file specified.
        Double rateLimit = null; // if non-null, samples less than this long after the previous one (with the same mux tag) are discarded.
        Tagset setTagsOnAll = null; // if not null, all tagsets created will have these tags added.
        boolean createRBB = false;
        int lines = 0;
        boolean hasty = false;
        boolean unique = false;

        try
        {
            int iArg = 0;
            for (; iArg < args.length; ++iArg)
            {
                if (args[iArg].equals("-mux"))
                {
                    mux = true;
                }
                else if(args[iArg].equalsIgnoreCase("-create")) {
                    createRBB = true;
                }
                else if(args[iArg].equals("-hasty")) {
                    hasty = true;
                }
                else if(args[iArg].equals("-infile"))
                {
                    ++iArg;
                    input = new BufferedReader(new FileReader(args[iArg]));
                }
                else if(args[iArg].equals("-rateLimit")) {
                    ++iArg;
                    rateLimit = Double.parseDouble(args[iArg]);
                }
                else if(args[iArg].equals("-setTags")) {
                    ++iArg;
                    setTagsOnAll = new Tagset(args[iArg]);
                }
                else if(args[iArg].equals("-unique")) {
                    unique = true;
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

            if (args.length - iArg != 1)
            {
                System.err.println(usage);
                System.exit(1);
            }

            String jdbc = args[iArg];

            if(hasty)
                jdbc += hastyOptions;

            RBB rbb0;

            if(createRBB)
                rbb0 = RBB.createOrConnect(jdbc);
            else {
                try {
                    rbb0 = RBB.connect(jdbc);
                } catch(SQLException e) {
                    System.err.println("Note: if the rbb doesn't exist yet, try the -create option, or run the 'create' command first.");
                    throw e;
                }
            }
            final RBB rbb = rbb0;


            if(input == null)
                input = new java.io.BufferedReader(new java.io.InputStreamReader(System.in));


            class TimeSeriesInfos
                extends java.util.HashMap<String, TimeSeriesInfo>
            {
                // if a timeseries was in reportProgress with the specified mux tag, end it.

                public void end(String muxtag)
                    throws SQLException
                {
                    TimeSeriesInfo ts0 = this.get(muxtag);
                    if (ts0 == null)
                        return;

                    this.remove(muxtag);

                    ts0.end(rbb);

                }

                public void endAll() throws SQLException
                {
                    // terminating sequences explicitly with an empty line (or a line of just the mux tag, if muxing) 
                    // is good because put doesn't have to keep around a list of sequences just in case additional observations are added later.
                    if(this.size() > 1) {
                        System.err.println("Warning: put ended with " + this.size() + " un-terminated timeseries.  Using last sample time as end time.");
                    }

                    while(!this.isEmpty()) {
                        end(this.keySet().iterator().next());
                    }
                }
            }

            TimeSeriesInfos timeSeries = new TimeSeriesInfos();

            while (true)
            {
                String line = input.readLine();
                if (line == null)
                {
                    break;
                }
                // System.out.println("I read " + line);

                if(++lines % 100000 == 0)
                    rbb.db().createStatement().execute("commit");

                String muxtag = "";

                if (mux)
                {
                    final String[] a = line.split(" ");
                    if (a.length == 1)
                    {
                        muxtag = a[0];
                        line = "";
                    }
                    else if (a.length == 2)
                    {
                        muxtag = a[0];
                        line = a[1];
                    }
                    else
                    {
                        throw new Exception("Error reading line \"" + line
                            + "\": expecting muxtag[ t,x1,x2,...]");
                    }
                }

                if (line.indexOf('=') >= 0) // it's taglist - make an event or start a timeseries
                {
                    // System.err.println("tagset:"+line);
                    boolean setTagsOnCurrent = false;
                    if(line.startsWith("+")) { // + at start of taglist means add tags to existing event.
                        setTagsOnCurrent = true;
                        line = line.substring(1);
                        // System.err.println("Adding tags:"+line);
                    }

                    // call takeDoubleValue on start, and, and time so none remain in the tagset.
                    Tagset tags = new Tagset(line);
                    if(setTagsOnAll != null)
                        tags.add(setTagsOnAll);
                    Double start = takeDoubleValue(tags, "start");
                    Double end   = takeDoubleValue(tags, "end");
                    Double time  = takeDoubleValue(tags, "time");
                    if(time != null) {
                        if(end != null || start != null) {
                            throw new Exception("Error in line \""+line+"\": if 'time' is specified, neither 'start' nor 'end' may be.");
                        }
                        start = end = time;
                    }
                    // these are magic tags and are not stored as tags.
                    tags.remove("start");
                    tags.remove("end");
                    tags.remove("time");
                    tags.remove("RBBID");  // this tag is synthesized by Get.  The ID of the stored timeseries will be different.

                    TimeSeriesInfo ts = timeSeries.get(muxtag);

                    if(setTagsOnCurrent && ts != null) { // special case - set tags of an ongoing Timeseries
                        ts.setTags(rbb, tags);
                    }
                    else {
                        if(setTagsOnCurrent) {
                            System.err.print("Warning: requested add tags to mux:"+muxtag+" but there was no timeseries in progress");
                        }

                       // if there was already a timeseries going and we're not adding tags to it, end it.
                        if(ts != null) {
                            timeSeries.end(muxtag);
                            ts = null;
                        }

                        // start the new one (+ for addTagsByID has no effect if there was no sequence already going.)
                        ts = new TimeSeriesInfo(tags, start, end);

                        if(unique && Event.find(rbb.db(), RBBFilter.byTags(tags)).length > 0) {
                            System.err.println("Discarding Timeseries "+tags+" because it is not unique");
                            ts.discard = true;
                        }

                        // even if the timeseries will be discarded because it is not unique,
                        // it must still be tracked to interpret the input correctly.
                        timeSeries.put(muxtag, ts);
                    }
                }
                else if (line.length() == 0) // empty line ends a sequence.
                {
                    timeSeries.end(muxtag);
                }
                else // otherwise it's timeseries data.
                {
                    TimeSeriesInfo ts = timeSeries.get(muxtag);
                    if (ts == null)
                    {
                        if (muxtag.equals(""))
                        {
                            throw new Exception(
                                "Error in line \"" + line
                                + "\": timeseries data must be preceeded by a taglist");
                        }
                        else
                        {
                            throw new Exception("Error in line \"" + line
                                + "\": timeseries data for muxtag \"" + muxtag
                                + "\" must be preceeded by a taglist");
                        }
                    }

                    final String[] string_data = line.split(",");
                    final double t = Double.parseDouble(string_data[0]); // first value is time
                    final Float[] data = new Float[string_data.length - 1];
                    for (int i = 0; i < data.length; ++i)
                        data[i] = Float.parseFloat(string_data[i + 1]);

                    ts.add(rbb, t, data, rateLimit);

                }
            }
            timeSeries.endAll();

            rbb.disconnect();
        }
        catch (Exception e)
        {
            System.err.println("Got Exception: " + e.getMessage());
            // e.printStackTrace();

        }
    }

    /**
     * 
     * @param name
     * @return
     * @throws SQLException
     */
    private static Double takeDoubleValue(Tagset t, String name) throws NumberFormatException
    {
        String s = t.getValue(name);
        if(s == null) {
            return null;
        }
        t.remove(name);
        return Double.parseDouble(s);
    }

    /*
     * from: http://h2database.com/html/performance.html#fast_import
     */
    private static final String hastyOptions = ";LOG=0;CACHE_SIZE=65536;LOCK_MODE=0;UNDO_LOG=0";

    private static final String usage = "Put [options] dbURL\n" +
            "Options:\n"+
            "\t-addTags [-addTags name=value]: add the specified tagset to all timeseries created\n"+
            "\t-create: create the RBB if necessary\n"+
            "\t-hasty: speed up inserts using H2 options that should only be used for exclusive access: "+hastyOptions+"\n"+
            "\t\tNote: Speed is also increased by using file (not server) access, i.e. jdbc:h2:file:...\n"+
            "\t-infile <file>: read from a file instead of stdin\n"+
            "\t-mux: first token of each line is an arbitrary ID that identifies the lines relevant to a particular timeseries\n"+
            "\t-rateLimit <1.23>: ignore any sample less than this long after its predecessor\n"+
            "\t-unique: do not create an Event (or Timeseries) if another Event already in the RBB has the same Tagset\n"+
            "\n"+
            "### Typical timeseries input:\n"+
            "tagname=tagvalue,tagname2=tagvalue2\n"+
            "0,111,112\n"+
            "1,121,122\n"+
            "\t\t# note: a blank line ends the timeseries immediately\n"+
            "tagname3=tagvalue3,tagname4=tagvalue4\n"+
            "0,211,212\n"+
            "1,221,222\n"+
            "\n"+
            "### Mixing input of two timeseries using -mux option:\n"+
            "joe tagname=tagvalue,tagname2=tagvalue2\n"+
            "bob tagname3=tagvalue3,tagname4=tagvalue4\n"+
            "joe 0,111,112\n"+
            "bob 0,211,212\n"+
            "joe 1,121,122\n"+
            "joe\t\t# note: this ends joe\n"+
            "bob 1,221,222\n"+
            "bob\t\t# note: this ends bob\n"+
            "\n"+
            "### To create events instead of timeseries, specify 'start' and 'end' in the tagset:\n"+
            "# note, 'start' and 'end' are stripped from the tagset before it is stored.\n"+
            "start=1,end=2,name=myEvent\n"+
            "# If start and end are the same, you can just use 'time':\n"+
            "time=6,name=DinnerTime\n"+
            "\n"+
            "### To add tags to an in-progress timeseries, prefix a tagset with +:\n"+
            "tagname=tagvalue\t\t# start a timeseries...\n"+
            "1,11,12\n"+
            "+tagname2=tagvalue2\t\t# this doesn't start a new timeseries, it adds tags to the ongoing timeseries.  Note: -unique does NOT handle this usage!\n"+
            "2,21,22\t\t# resume adding samples...\n";



}


/*
 * This is not an innner class because that gives Put access to its private members,
 * which might make it harder to enforce the 'discard' flag for example.
 */
class TimeSeriesInfo
{
    TimeSeriesInfo(Tagset tags_,
        Double start_,
        Double end_)
    {
        tags = tags_;
        specifiedStart = start_;
        specifiedEnd = end_;
        samples = new ArrayList<Float[]>();
        sampleTimes = new ArrayList<Double>();
        discard = false;
    }

    void add(RBB rbb, double t, Float[] data, Double rateLimit) throws SQLException {
        if(discard)
            return;

        timeOfLastSampleOffered = t;

        if (rbbID == null)
        { 
            // this is the first observation.
            // A reason for not waiting until flushSamples() to create the
            // timeseries is the -unique tag; a timeseries should not be pre-empted
            // by one created later even if the later one is flushed first.
            rbbID = H2STimeseries.start(rbb.db(), data.length, specifiedStart==null ? t : specifiedStart, tags.toString()); // use time of first observation as start if no start time was specified by the special start tag.
        }

        if(data.length == 0) // a line can have just a time, which extends the end time.
            return;

        if(rateLimit != null && timeOfLastSampleAccepted != null &&
            t - timeOfLastSampleAccepted < rateLimit) {
            return;
        }

        timeOfLastSampleAccepted = t;
        sampleTimes.add(t);
        samples.add(data);
        if(samples.size() >= 1000)
           flushSamples(rbb);
    }

    void end(RBB rbb) throws SQLException {
        if(discard)
            return;

        if(timeOfLastSampleOffered == null) { // not no samples.  If start/end times were specified, it is assumed to be an Event.
            if(specifiedStart != null && specifiedEnd != null)
                H2SEvent.create(rbb.db(), specifiedStart, specifiedEnd, tags.toString());
            else
                System.err.println("Put Warning: got tags \""+tags+"\" but no samples for it and no "+(specifiedStart==null?"Start":"End")+" time.  Not creating an Event/Timeseries.");
        }
        else {
            flushSamples(rbb);
            H2SEvent.setEndByID(rbb.db(), rbbID, specifiedEnd != null ? specifiedEnd : timeOfLastSampleOffered); // if no end time was specified use the time of the final sample.
        }
    }

    void setTags(RBB rbb, Tagset tags) throws SQLException {
        this.tags.set(tags);

        if(discard)
            throw new SQLException("Error: used the + syntax to add tags to a Timeseries, in combination with the -unique option.");

        H2SEvent.setTagsByID(rbb.db(), rbbID, tags.toString()); // note: start,end,time tags will be ignored if adding tags to in-progress timeseries
    }

    private void flushSamples(RBB rbb) throws SQLException {
        assert(!discard); // shouldn't be called if discarding.
        if(samples.isEmpty())
            return;
        // System.err.println("Flushing "+samples.size()+" samples");
        H2STimeseries.addSamplesByID(rbb.db(), rbbID, sampleTimes.toArray(new Double[0]), samples.toArray(new Float[0][]), null, null);
        sampleTimes.clear();
        samples.clear();
    }

    boolean discard; // true if the data should be discarded, e.g. because -unique was used and this is a duplicate.

    private Tagset tags;
    /*
     * These are the start/end times specified by the special 'start', 'end', and 'time' tags.
     * If not specified, these will be null and, for timeseries, we will use the first/last
     * Sample times instead.
     */
    private Double specifiedStart, specifiedEnd;
    /*
     * Time of the most recent sample, i.e. previous call to add()
     */
    private Double timeOfLastSampleOffered;
    /**
     * Time of the most recent sample added to 'samples.'
     * This is just the last element of sampleTimes, except when
     * samples/sampleTimes has just been flushed to the DB.
     */
    private Double timeOfLastSampleAccepted;
    
    private Long rbbID;

    /*
     * These are the times of samples currently buffered for writing to the DB.
     */
    private ArrayList<Double> sampleTimes;
    private ArrayList<Float[]> samples;
}
