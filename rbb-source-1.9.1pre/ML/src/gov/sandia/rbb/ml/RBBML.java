/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml;

import gov.sandia.rbb.tools.RBBValues;
import gov.sandia.rbb.RBBFilter;
import gov.sandia.rbb.Timeseries;
import gov.sandia.rbb.ml.features.PrintObservationFE;
import gov.sandia.rbb.impl.h2.statics.H2STimeseries;
import gov.sandia.rbb.impl.h2.statics.H2STime;
import java.sql.SQLException;
import java.io.StringWriter;
import gov.sandia.rbb.impl.h2.statics.H2SRBB;
import gov.sandia.rbb.ml.features.ProblemAgeFE;
import gov.sandia.rbb.ml.features.ObserveTimespanFE;
import java.sql.PreparedStatement;
import gov.sandia.rbb.impl.h2.statics.H2SEvent;
import gov.sandia.rbb.Event;
import gov.sandia.rbb.tools.RBBSelection;
import gov.sandia.rbb.ml.features.BufferObservationsFE;
import gov.sandia.rbb.RBB;
import gov.sandia.rbb.Tagset;
import static gov.sandia.rbb.Tagset.TC;
import gov.sandia.rbb.ml.features.GnuplotFE;
import gov.sandia.rbb.ui.RBBReplayControl;
import gov.sandia.rbb.util.StringsWriter;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import static gov.sandia.rbb.ml.RBBML.MLPart.*;
import gov.sandia.rbb.ml.RBBML.Mode;
import static gov.sandia.rbb.ml.RBBML.Mode.*;
import static gov.sandia.rbb.RBBFilter.*;

/**
 *
 * RBBML is the class that connects (potentially) several different RBBs
 * to do learning.
 *
 * The RBBs are opened lazily because some RBBML functions don't require all the RBBs.
 * 1) If the (exact) same JDBC URL is specified for two different parts, they share the same underlying RBB
 * 2) If a part is never used, its RBB is never opened.
 * 
 *
 * @author rgabbot
 */
public class RBBML {

    /**
     * This is for calling MLMain from the command line - from java code call MLMain instead.
     * If there is an exception, it prints out the message and aborts the program with an error code.
     */
    public static void main(String[] args) {
        try
        {
            RBBMLMain(args);
        }
        catch(Throwable e)
        {
            System.err.println(e.getClass().getName() + " Error: "+e.getMessage());
            if(e instanceof java.lang.reflect.InvocationTargetException) {
                java.lang.reflect.InvocationTargetException ite = (java.lang.reflect.InvocationTargetException) e;
                    System.err.println(ite.getCause());
            }
            System.exit(-1);
        }       
    }

    /**
     * unlike main(), MLMain() propagates exceptions so it is more useful for
     * calling from other code or unit tests.
     *
     * To call it from other code you will typically:
     * import static gov.sandia.rbb.ml.MLMain.MLMain;
     *
     */
    public static void RBBMLMain(String... args) throws Exception
    {
        final String username = "John Doe";

        String usage =
                    "MLMain <-RBBs> <command> [args...]\n"+
                    "RBBs:  specify one or more RBBs for the RBB ML command that follows.\n"+
                    "   -rbb <JDBC_URL>: The default for any of the other RBB options (session, model, predictions, coordination) that are not specified:\n"+
                    "   -sessionRBB <JDBC_URL>: The source data being modeled.\n"+
                    "   -modelRBB <JDBC_URL>:  Where the training data and selected features will be stored.\n"+
                    "   -predictionsRBB <JDBC_URL>: Where model predictions (e.g. flags or generated behaviors) will be stored.\n"+
                    "   -coordinationRBB <JDBC_URL>: Synchronizes the current time, and user selection of training data.\n"+
                    "commands:  (invoke a command without arguments for more detail)\n"+
                    "   addFeatures - Make the model use the specified feature(s)\n"+
                    "   create - Create a new RBB ML model.\n"+
                    "   deleteTrainingData - Delete training data from the specified Training Event from the model.\n"+
                    "   deselectAll - De-select any currently selected data.\n"+
                    "   getFeatures - Specify and apply a feature extraction chain (without creating a Model)\n"+
                    "   nextFlag - Jump to the time of the next flag for the specified model\n"+
                    "   observe - Perform feature extraction for the model from the session, sending the results to the console, a plot, or a table.\n" +
//                    "   plot - Plot model training features for  (does't predict; see predict -plot)\n"+
                    "   prevFlag - Jump to the time of the previus flag for the specified model\n"+
                    "   print - Print information about the model (and Session, if specified)\n" +
                    "   removeFeatures - The model will disregard the specified feature(s)\n"+
//                    "   reset - Delete all training examples and training data from the Session and model\n"+
                    "   select [tagset] <tagset2...> - Sequentially select Events matching the specified tagset(s)\n"+
                    "   setSimTime - Jump to specified scenario time\n"+
                    "   storePredictionInputs - Store a set of parameters for the -storedInputs option of the predict command.\n"+
                    "   train - Add the selected example in the Session to the model\n"+
                    "   predict - Apply the model to data\n";

        RBBML m = new RBBML();
        args = m.setRBBs(args);

        // strip command from args
        if(args.length == 0)
            throw new Exception(usage);

        final String cmd = args[0];
        args = Arrays.copyOfRange(args, 1, args.length);

        // strip rbb specifications from args and initialize the getModel().
        // this is harmless since RBBs are only actually opened if and when needed.

        // process the command.
        if(cmd.equalsIgnoreCase("train"))
        {
            usage =
                "train [-timeStep <dt>] <label>\n"+
                "   -timeStep <dt>: specify a timestep; otherwise only the sample times of the first timeseries are used.\n";

            Double timestep=null;

            if(args.length==0)
                throw new Exception(usage);

            int iArgs=0;
            for(; iArgs < args.length-1 && args[iArgs].startsWith("-"); ++iArgs) { // -1 because last arg is the label, which could start with "-" e.g. -1
                if(args[iArgs].equalsIgnoreCase("-timestep")) {
                    if(++iArgs >= args.length)
                        throw new Exception("Error: -timestep requires an argument:\n"+usage);
                    timestep = m.parseTime(args[iArgs]);
                }
                else {
                    throw new Exception("RBBMLMain.train Error: unrecognized argument " + args[iArgs]);
                }
            }

            if(iArgs < args.length-1)
                throw new Exception("Error: wrong number of arguments.\n"+usage);

            m.addSelectedTrainingData(args[iArgs]);
        }
//        else if(cmd.equalsIgnoreCase("deleteTrainingData"))
//        {
//            usage = "deleteTrainingData <modelRBB> <TRAINING_EVENT_ID>";
//            if(args.length != 2)
//                throw new Exception("Error: deleteTrainingData command requires 2 args:\n"+usage);
//
//            MLModel model = MLgetModel().open(args[0], username);
//            Long ID = Long.parseLong(args[1]);
//
//            getModel().deleteTrainingExampleByID(ID);
//        }
        else if(cmd.equalsIgnoreCase("storePredictionInputs"))
        {
            usage =
                "storePredictionInputs <input> [input]... [resultTags]\n"+
                "   Each input is a tagset.  The number of tagsets must equal the number\n"+
                "   of Prediction Inputs for the model.  If resultTags are specified,\n"+
                "   they will be added to all results from applying prediction on these inputs.\n";

            final String[] inputNames = m.getModel().getInputNames(PREDICTION);

            ArrayList<Tagset> inputTags = new ArrayList<Tagset>();
            ArrayList<Tagset> groupTags = new ArrayList<Tagset>();

            int iArgs=0;
            for(; iArgs < inputNames.length && iArgs < args.length; iArgs++) {
                if(m.getModel().isGroupInput(inputNames[iArgs]))
                    groupTags.add(TC(args[iArgs]));
                else
                    inputTags.add(TC(args[iArgs]));
            }

            if(inputTags.size() + groupTags.size() < inputNames.length)
                throw new Exception("Error: the model "+m.getModel().getName() + " requires a tagset for each of: "+StringsWriter.join(" ", inputNames));

            Tagset resultTags = TC("");
            if(iArgs < args.length)
                resultTags = TC(args[iArgs++]);
            if(iArgs < args.length)
                throw new Exception("unexpected extra parameters to storeInputs command.  Usage:\n"+usage);
            m.createProblemSet(inputTags.toArray(new Tagset[0]), groupTags.toArray(new Tagset[0]), resultTags); // empty tagset = no resultTags added.
        }
        else if(cmd.equalsIgnoreCase("create"))
        {
            usage = "create <ModelJavaClass> <ModelName>";
            if(args.length != 2)
                throw new Exception("Error: create requires 2 arguments:\n"+usage);
            RBB rbb = RBB.create(m.getURLForPart(MODEL), args[1]);
            MLModel.create(rbb, args[0], username);
            rbb.disconnect();
        }
//        else if(cmd.equalsIgnoreCase("reset"))
//        {
//            if(args.length != 2)
//                throw new Exception("Error: reset command requires 2 args:\n"+usage);
//
//            ml = new RBBML(args[0], args[1], username);
//            ml.reset();
//        }
        else if(cmd.equalsIgnoreCase("deselectAll")) {
            m.deselectAll();
        }
        else if(cmd.equalsIgnoreCase("select")) {
            if(args.length==0)
                throw new Exception("Error: select requires at least one tagset.");
            for(String tags : args)
                for(Event event : Event.find(m.getRBB(SESSION).db(), byTags(tags)))
                    RBBSelection.oneShot(m.getRBB(SESSION).db(), m.getRBB(COORDINATION).db()).selectEvents(byID(event.getID()));
        }
        else if(cmd.equalsIgnoreCase("print"))
        {
            if(args.length > 0)
                throw new Exception("Extraneous args to print: "+StringsWriter.join(", ", args));
            // any part for which a URL is provided will be printed; others will not.
            System.out.println(m.toStringDetail());
        }
        else if(cmd.equalsIgnoreCase("nextFlag"))
        {
            if(!m.advanceToFlag(nextFlag))
                System.err.println("No more flags in scenario.");
        }
        else if(cmd.equalsIgnoreCase("prevFlag"))
        {
            if(!m.advanceToFlag(prevFlag))
                System.err.println("No previous flags in scenario.");
        }
        else if(cmd.equalsIgnoreCase("plot")) {
            usage =
                "plot <options...>\n"+
                "   -timestep <1>: if not specified, the sample times of the first input are used.\n"+
                "   -start <0>: a time (e.g 10.3) or 'now'.  If not specified, use the problem instance from its beginning.\n"+
                "   -end <1>: a time (e.g 12.3) or 'now'.  If not specified, use the problem instance until its end.\n";

            Double start = null, end = null;

            int iArgs=0;
            while(iArgs < args.length && args[iArgs].startsWith("-")) {
                if(args[iArgs].equalsIgnoreCase("-start")) {
                    if(++iArgs >= args.length)
                        throw new Exception("Error: -start requires an argument:\n"+usage);
                    start = m.parseTime(args[iArgs]);
                }
                else if(args[iArgs].equalsIgnoreCase("-end")) {
                    if(++iArgs >= args.length)
                        throw new Exception("Error: -end requires an argument:\n"+usage);
                    end = m.parseTime(args[iArgs]);
                }
                else if(args[iArgs].equalsIgnoreCase("-timestep")) {
                    if(++iArgs >= args.length)
                        throw new Exception("Error: -timestep requires an argument:\n"+usage);
                    end = m.parseTime(args[iArgs]);
                }
                else {
                    throw new Exception("Error: unrecognized arg \"" + args[iArgs] + "\":\n"+usage);
                }
            }

            if(iArgs >= args.length)
                throw new Exception("Error: no features specified: "+usage);

            m.observeSelectedTrainingExample(new GnuplotFE(null, args, null));
        }
        else if (cmd.equalsIgnoreCase("observe")) {
            usage =
                "observe  [-start <start>] [-end <end>]\n"+
                "   -start: a time (e.g 10.3) or 'now'.  If not specified, use the problem instance from its beginning.\n"+
//                "   -storedInputs: apply feature extraction to all the stored inputs for the model, instead of currently-selected data.\n"+
                "   -end: a time (e.g 12.3) or 'now'.  If not specified, use the problem instance until its end.\n";
            Double start = null, end = null, timeStep = null;
            int iArgs=0;
            while(iArgs < args.length && args[iArgs].startsWith("-")) {
                if(args[iArgs].equalsIgnoreCase("-start")) {
                    if(++iArgs >= args.length)
                        throw new Exception("Error: -start requires an argument:\n"+usage);
                    start = m.parseTime(args[iArgs]);
                }
                else if(args[iArgs].equalsIgnoreCase("-end")) {
                    if(++iArgs >= args.length)
                        throw new Exception("Error: -end requires an argument:\n"+usage);
                    end = m.parseTime(args[iArgs]);
                }
//                else if(args[iArgs].equalsIgnoreCase("-storedInputs")) {
//                    ArrayList<ProblemSet> ps = m.getProblemSets();
//                    if(ps.isEmpty())
//                        System.err.println("Warning: -storedInputs used, but there are no stored inputs.  Use the storePredictionInputs command in MLMain.");
//                    else
//                        problemSets.addAll(ps);
//                }
                else {
                    throw new Exception("Error: unrecognized arg \"" + args[iArgs] + "\":\n"+usage);
                }
            }
            ArrayList<MLObservationSequence> obs = new ArrayList<MLObservationSequence>();
            m.observeSelectedTrainingExample(new BufferObservationsFE(obs, null));
            if(obs.size() != 1) {
                throw new Exception("Error!  Must first select: "+StringsWriter.join(" ", m.getModel().getInputNames(TRAINING)));
            }
            MLObservationSequence ob = obs.get(0);
            for(int age = 0; age < ob.size(); ++age)
                System.out.println(ob.getOldest(age));
        }
        else if(cmd.equalsIgnoreCase("addFeatures")) {
            usage = "addFeatures <feature1[,...]>";
            if(args.length  < 1)
                throw new Exception("Error: addFeatures command requires 1 or more features to add:\n"+usage);
            Set<String> featuresBefore = new java.util.HashSet<String>(Arrays.asList(m.getModel().getPredictorNames()));
            Set<String> featuresAfter = new java.util.HashSet<String>(Arrays.asList(args[0].split(",")));
            featuresAfter.addAll(featuresBefore);
            if(featuresAfter.size()==featuresBefore.size())
                throw new Exception("All the specified features were already in the model");
            m.getModel().setPredictorNames(featuresAfter.toArray(new String[0]));
        }
        else if(cmd.equalsIgnoreCase("removeFeatures")) {
            if(args.length  < 1)
                throw new Exception("Error: removeFeatures command requires 1 or more features to remove:\n"+usage);
            Set<String> featuresBefore = new java.util.HashSet<String>(Arrays.asList(m.getModel().getPredictorNames()));
            Set<String> featuresAfter = new java.util.HashSet<String>(Arrays.asList(m.getModel().getPredictorNames()));
            featuresAfter.removeAll(Arrays.asList(args[0].split(",")));
            if(featuresAfter.size()==featuresBefore.size())
                throw new Exception("None of the specified features were in the model");
            m.getModel().setPredictorNames(featuresAfter.toArray(new String[0]));
        }
        else if(cmd.equalsIgnoreCase("setSimTime")) {
            usage = "setSimTime [startTime] <time>";
            if(args.length == 1)
                m.setSimTime(Double.parseDouble(args[0]));
            else if(args.length == 2) {
                double start = Double.parseDouble(args[0]);
                double end   = Double.parseDouble(args[1]);
                m.setHistoryLength(end-start);
                m.setSimTime(end);
            }
            else {
                throw new Exception("Error: setSimTime command requires either 1 arg (a time to select) or 2 args (start/end of selected time):\n" + usage);
            }
        }
        else if(cmd.equalsIgnoreCase("predict")) {
            m.predict(args);
        }
        else if(cmd.equalsIgnoreCase("getFeatures")) {
            usage = "getFeatures [-online] [-start 1.23] [-end 1.23] [-input name tag1=value1]... [-group name tag=value]... [-trainingFeatures <modelClass>] <-featureFE arg1:arg2:arg3...>...";
            ArrayList<String> inputNames = new ArrayList<String>();
            ArrayList<RBBFilter> inputs = new ArrayList<RBBFilter>();
            ArrayList<String> groupNames = new ArrayList<String>();
            ArrayList<RBBFilter> groups = new ArrayList<RBBFilter>();
            String timeCoordinate = null;
            Double start=null, end=null, timestep=null;
            boolean batch = true;
            MLFeatureExtractor fe = new MLFeatureExtractor(null); // this is a no-op feature extractor.
            int i = 0;
            for(; i < args.length; ++i) {
                 if(args[i].equalsIgnoreCase("-input")) {
                    if(args.length < i+3)
                        throw new Exception("getFeature s -input requires 2 args: <inputName> <filterTags>");
                    inputNames.add(args[++i]);
                    inputs.add(byTags(args[++i]));
                }
                else if(args[i].equalsIgnoreCase("-group")) {
                    if(args.length < i+3)
                        throw new Exception("getFeature s -inputGroup requires 2 args: <inputName> <filterTags>");
                    groupNames.add(args[++i]);
                    groups.add(byTags(args[++i]));
                }
                else if(args[i].equalsIgnoreCase("-start")) {
                    if(args.length < i+1)
                        throw new Exception("-start requires a floating-point number");
                    start =  m.parseTime(args[++i]);
                }
                else if(args[i].equalsIgnoreCase("-end")) {
                    if(args.length < i+1)
                        throw new Exception("-end requires a floating-point number");
                    end = m.parseTime(args[++i]);
                }
                else if(args[i].equalsIgnoreCase("-timestep")) {
                    if(args.length < i+1)
                        throw new Exception("-timestep requires a floating-point number");
                    timestep = Double.parseDouble(args[++i]);
                }
                else if(args[i].equalsIgnoreCase("-modelTrainingFeatures")) {
                    if(args.length < i+1)
                        throw new Exception("-modelTrainingFeatures requires a model class name");
                    fe.addToChain(MLModel.instantiateModel(args[++i]).getTrainingFE());
                }
                else if(args[i].equalsIgnoreCase("-online")) {
                    batch=false;
                }
                else if(args[i].equalsIgnoreCase("-timeCoordinate")) {
                    if(args.length < i+1)
                        throw new Exception("-timeCoordinate requires a tagset including the tag timeCoordinate=");
                    timeCoordinate = args[++i];
                }
                else if(args[i].startsWith("-")) {
                    final String featureClass = args[i].substring(1); // all but initial '-'
                    if(++i >= args.length)
                        throw new Exception("No parameters specified for "+featureClass);
                    fe.addToChain(MLFeatureExtractor.fromStrings(featureClass, args[i].split(":")));
                }
                else {
                    throw new Exception("ml getFeatures: unrecognized arg \""+args[i]+"\"");
                }
            }
            if(inputNames.isEmpty())
                throw new Exception(usage+"\nAt least one (non-group) input is required.");

            if(batch) {
                m.batch(
                    inputNames.toArray(new String[0]), inputs.toArray(new RBBFilter[0]),
                    groupNames.toArray(new String[0]), groups.toArray(new RBBFilter[0]),
                    null, fe, start, end, timestep, timeCoordinate);
            }
            else {
                if(timeCoordinate != null)
                    throw new Exception("Error: online prediction doesn't support use of a time coordinate.");
                new MLOnline(m,
                    inputNames.toArray(new String[0]), inputs.toArray(new RBBFilter[0]),
                    groupNames.toArray(new String[0]), groups.toArray(new RBBFilter[0]),
                    fe, null);
            }
        }
        else {
            throw new Exception("Unrecognized command \""+cmd+"\": "+usage);
        }

//        if(ml != null)
//            ml.close();
    }

//    private void cacheTimeseries(MLPart p, Tagset filterTags, Tagset timeCoordinate) throws Exception {
//        TimeseriesCache cache = new TimeseriesCache(getRBB(SESSION));
//        System.err.println("initTimeseriesCache " + filterTags);
//        cache.initTimeseriesCache(filterTags, timeCoordinate, null);
//        RBBs.put(getURLForPart(SESSION), cache);
//    }

    /**
     * This is allocated lazily by getModel(), so it should only be accessed by getModel()
     * in other words, write getModel().xxx(), not model.xxx()
     */
    private MLModel model;

    /*
     * Get the RBB for the part as specified by the args to attachRBBs.
     *
     * This function maintains the RBBs hash to ensure only one RBB is allocated per JDBC URL,
     * even if the same URL is specified for multiple parts.
     */
    public RBB getRBB(MLPart p) throws Exception {
        String url = getURLForPart(p);
        RBB rbb = RBBs.get(url);
        if(rbb==null)
            RBBs.put(url, rbb=RBB.connect(url));
        return rbb;
    }

    class PartNotSpecifiedException extends Exception {
        PartNotSpecifiedException(MLPart p) {
            super(p.argName+" required (or specify a default using "+MLPart.DEFAULT.argName+")");
        }
    }

    /**
     * Return the URL for the specified part.
     * Does not return null; throws PartNotSpecified if neither a specific nor default url was specified.
     */
    private String getURLForPart(MLPart p) throws Exception {
        String u = URLs.get(p);
        if(u==null && p != MLPart.DEFAULT)
            u = URLs.get(MLPart.DEFAULT);
        if(u==null)
            throw new PartNotSpecifiedException(p);
        return u;
    }

    /**
     * True iff this was initialized with a JDBC url for the specified part,
     * or a default URL (using -rbb)
     */
    private boolean wasPartSpecified(MLPart p) {
        return URLs.containsKey(p) || URLs.containsKey(DEFAULT);
    }

    public enum MLPart {
        SESSION("-sessionRBB"),
        MODEL("-modelRBB"),
        PREDICTIONS("-predictionsRBB"),
        COORDINATION("-coordinationRBB"),
        DEFAULT("-rbb");

        String argName;
        MLPart(String argName) {
            this.argName = argName;
        }
    };

    /**
     *
     * Many operations in the RBBML package can be categorized
     * as training (adding training examples) or prediction.
     * <p>
     * You may want to do both:<br>
     * import gov.sandia.rbb.ml.RBBML.Mode;
     * import static gov.sandia.rbb.ml.RBBML.Mode.*;
     * 
     */
    public enum Mode { TRAINING, PREDICTION };

    /*
     * Map each part to the URL the caller specified for it.
     */
    Map<MLPart, String> URLs;

    /**
     * Map each RBB JDBC URL to an RBB instance.
     */
    Map<String, RBB> RBBs;

    /**
     * Time/replay control, allocated on-demand by getReplayControl()
     * (do not access this.replayControl directly)
     */
    private RBBReplayControl replayControl;

    RBBReplayControl getReplayControl() throws Exception {
        if(replayControl == null) {
            // RBBML doesn't do any event-driven processing in response to changes
            // to the current time, so we use the non-listener form of the constructor.
            replayControl = new RBBReplayControl(getRBB(COORDINATION));
        }
        return replayControl;
    }

    /**
     * Strips the following JDBC_URL args from the parameter and returns unused args.
     * -rbb <JDBC_URL>: Provides a default for any of the other RBB options that are not specified:
     *    -sessionRBB <JDBC_URL>
     *    -modelRBB <JDBC_URL>
     *    -predictionsRBB <JDBC_URL>
     *    -coordinationRBB <JDBC_URL>
     *
     */
    public String[] setRBBs(String... args) throws Exception {
        ArrayList<String> unusedArgs = new ArrayList<String>();

        URLs = new HashMap<MLPart, String>();
        RBBs = new HashMap<String, RBB>();

        StringWriter sw = new StringWriter();
        sw.write("RBBML using: ");

        ARGS: for(int i = 0; i < args.length; ++i) {
            for(MLPart p : MLPart.values())
                if(args[i].equalsIgnoreCase(p.argName)) {
                    if(++i >= args.length)
                        throw new Exception(p.argName+" requires a JDBC URL");
                    URLs.put(p, args[i]);
                    sw.write(" "+args[i-1]+" "+args[i]);
                    continue ARGS;
                }
            unusedArgs.add(args[i]);
        }

        // System.err.println(sw);

        return unusedArgs.toArray(new String[0]);
    }


    public static final String predictUsageString =
        "predict [-standardOptions..] [-modelSpecificOptions...]: run the model on the specified inputs and print timeseries for each resulting problem.\n"+
        "  standardOptions:\n"+
        "   -online: Apply only to new data, as it arrives (as opposed to default batch mode)\n"+
        "       NOTE: if run online, the program won't exit, it waits for new inputs until it is interrupted (or the code calls H2EventTCPServer.stopAll())\n"+
        "   -deleteOldResults: Erase from the session any Events/Timeseries from previous -store or -storeEvent actions on this model.\n"+
        "   -print: print the output to stdout instead of taking the model's normal action.\n"+
//        "   -store: store the output timeseries in the RBB.  Each output timeseries has the following tags, set in order:\n"+
//        "      1) The ID of each Input Timeseries as a tag, <inputName>.RBBID=<ID>\n"+
//        "      2) The model's result tags (getResultTags): model=<modelName>,type=result,\n"+
//        "      3) The tagset from the previous -resultTags option if used with -inputs, or the stored resultTags for -storedInputs.\n"+
//        "         These user-specified result tags can overwrite any of the other tags (or remove them using a null value),\n"+
//        "         but then -deleteOldResults won't recognize them later.\n"+
//        "         -resultTags may also include references to Input tags, of the form: tagname=<inputname>.<tagname>\n"+
//        "         The value of such tags will be substituted for the value of the specified tag from the specified input.\n"+
//        "   -storeEvents: like -store, but discards the timeseries data, storing only a discrete event for each result.\n"+
        "   -inputs <input> <input2...>: Each input is a tagset.  Prediction will be applied to every combination of RBB Events matching the inputs.\n"+
        "      This option can be specified multiple times, but each unique combination of events will only be evaluated the first time it matches.\n"+
        "   -storedInputs: Apply prediction to all the sets of inputs stored in the session (see the storeInputs command in RBBMLMain).\n";

    public void predict(String... args) throws Exception {

        if(args.length==0)
            throw new Exception(predictUsageString);

        // parse standard options.
        // The reason these are stored as booleans during arg parsing instead of simply
        // building the feature extraction chain directly is because they are always applied
        // in the same order, regardless of the order in which they are specified.
        boolean batch = true;
        boolean print = false;
        ArrayList<ProblemSet> problemSets = new ArrayList<ProblemSet>();
        Tagset resultTags = null;

        int iArg=0;
        for(; iArg < args.length; ++iArg) {
            if(args[iArg].equalsIgnoreCase("-deleteOldResults")) {
                Integer n  = deletePredictions();
                System.err.println(n.toString() + " old results deleted.\n");
            }
            else if(args[iArg].equalsIgnoreCase("-online")) {
                batch = false;
            }
            else if(args[iArg].equalsIgnoreCase("-print")) {
                print = true;
            }
            else if(args[iArg].equalsIgnoreCase("-inputs")) {

                final String[] inputNames = getModel().getInputNames(PREDICTION);

                if(iArg+inputNames.length >= args.length)
                    throw new Exception("Error: -inputs requres one tagset for each of: "+StringsWriter.join(" ", inputNames));

                ArrayList<RBBFilter> inputFilters = new ArrayList<RBBFilter>();
                ArrayList<RBBFilter> groupFilters = new ArrayList<RBBFilter>();

                for(int i = 0; i < inputNames.length; i++) {
                    iArg++;
                    if(getModel().isGroupInput(inputNames[i]))
                        groupFilters.add(RBBFilter.fromString(args[iArg]));
                    else
                        inputFilters.add(RBBFilter.fromString(args[iArg]));
                }

                ProblemSet ps = new ProblemSet();
                ps.predictionInputFilter = inputFilters.toArray(new RBBFilter[0]);
                ps.predictionGroupFilter = groupFilters.toArray(new RBBFilter[0]);
                ps.resultTags = resultTags;
                problemSets.add(ps);
            }
            else if(args[iArg].equalsIgnoreCase("-storedInputs")) {
                ArrayList<ProblemSet> ps = getProblemSets();
                if(ps.isEmpty())
                    System.err.println("Warning: -storedInputs used, but there are no stored inputs.  Use the storePredictionInputs command in MLMain.");
                else
                    problemSets.addAll(ps);
            }
            else {
                getModel().parsePredictionArgs(Arrays.copyOfRange(args, iArg, args.length));
                break; // all remaining args belong to the getModel().
            }
        }

        if(problemSets.isEmpty())
            throw new Exception("RBBML.predict error: no inputs specified.  Use either -inputs or -storedInputs.");

        MLFeatureExtractor fe = getModel().getPredictionFE();
        if(print)
            fe.addToChain(new PrintObservationFE("", null));
        else
            fe.addToChain(getModel().getActionFE());

        // this is used for batch processing to enforce the guarantee that each problem set is only processed once.
        ArrayList<Object[]> alreadyFound = new ArrayList<Object[]>();

        for(ProblemSet ps : problemSets) {

            MLFeatureExtractor fe0 = fe.clone(); // the feature extraction chain may be different for each Problem Set.

            resultTags=getModel().getResultTags().clone();

            for(String inputName : getModel().getInputNames(PREDICTION, false))
                resultTags.set(inputName+".RBBID", inputName+".RBBID"); // CreateEventFE or CreateTimeseriesFE will replace this special value with the id of the input

            if(ps.resultTags!=null)
                resultTags.set(ps.resultTags);  // user-specified result tags have the last word.

            if(batch) {
                ResultSet rs = batch(
                   getModel().getInputNames(PREDICTION, false), ps.predictionInputFilter,
                   getModel().getInputNames(PREDICTION, true), ps.predictionGroupFilter,
                       alreadyFound.toArray(),
                       fe0, null, null, getModel().getTimestep(), null);

                // add the results from this time to alreadyFound, so different ProblemSets
                // don't evaluate the same problem instance.
                while(rs.next())
                    alreadyFound.add((Object[])rs.getArray("IDS").getArray());
            }
            else {
                new MLOnline(this,
                        getModel().getInputNames(PREDICTION, false), ps.predictionInputFilter,
                        getModel().getInputNames(PREDICTION, true), ps.predictionGroupFilter, // todo: add support for stored group inputs.
                        fe0, 0.0);
            }
        }

//        if(batch)
//            System.err.println("RBBML.predict - found " + alreadyFound.size() + " problem instances");
    }


    /**
     * The user is the username of the person responsible for any changes to the getModel().
     * This is a string associated with any modifications to the model, such as adding
     * training examples or selecting features.
     *
     * In the future this could be used, for example, to train on only training examples added by a select
     * group of people, or to study how people differ in using the program.
     *
     */
    String user;

    public String toStringDetail() {
        try {
            StringsWriter sw = new StringsWriter();

            try { // MODEL
                sw.write(getModel().toStringDetail());
            } catch(PartNotSpecifiedException e) {
                sw.write("(skipping modelRBB; not specified)\n");
            }

            try { // SESSION
                sw.writeStrings("Session: ", getRBB(SESSION).getName(), "\n");
            } catch(PartNotSpecifiedException e) {
                sw.write("(skipping sessionRBB; not specified)\n");
            }

            try { // COORDINATION
                sw.writeStrings("SimTime: ", getSimTime().toString(), "\n");
            } catch(PartNotSpecifiedException e) {
                sw.write("(skipping coordinationRBB; not specified)\n");
            }

            try { // PREDICTIONS
                sw.write("Problem Sets in this session:\n");
                try {
                    sw.writeQueryResults(getRBB(PREDICTIONS).db(),
                        "select ID, MODEL, (RBB_IDS_TO_TAGSETS(INPUT_TAGS)) as INPUT_TAGS, RBB_IDS_TO_TAGSETS(GROUP_TAGS) as GROUP_TAGS, RBB_ID_TO_TAGSET(RESULT_TAGS) as RESULT_TAGS from RBBML_PROBLEMS");
                } catch(SQLException e){
                    // The RBBML_PROBLEMS table will not exist if no Stored Prediction Inputs have been created.
                }
                sw.write("Model Results in this session:\n");
                for(Event e : Event.find(getRBB(PREDICTIONS).db(), byTags(model.getMinimalResultTags())))
                    sw.writeStrings("\t", e.toString(), "\n");
                sw.write("\n");
            } catch(PartNotSpecifiedException e) {
                sw.write("(skipping predictionsRBB; predictionsRBB or modelRBB was not specified)\n");
            }

            return sw.toString();
        } catch(Exception e) {
            return new String("Exception in RBBML.toStringDetail(): " + e.toString());
        }
    }

    Double parseTime(String s) throws Exception {
        return s.equalsIgnoreCase("now") ? getSimTime() : Double.parseDouble(s);
    }

    /**
     * Retrieve the currently selectedInputs sim time.
     *
     */
    public Double getSimTime() throws Exception {
        return getReplayControl().getSimTime() / 1000.0;
    }

    public Double getHistoryLength() throws Exception {
        return RBBValues.oneShot(getRBB(COORDINATION), "historyLength").getFloat("before", 0.0f).doubleValue();
    }

    public void setHistoryLength(Double n) throws Exception {
        RBBValues.oneShot(getRBB(COORDINATION), "historyLength").setFloats("before", n.floatValue());
    }

    /**
     * Set the current sim time.
     *
     * The replay speed is left as previous.
     *
     */
    public void setSimTime(double t) throws Exception {
        getReplayControl().setSimTime((long)(t*1000));
    }

    /**
     * Go to the next flag (or previous, depending on whichFlag) from this getModel().
     *
     * Returns false if there are no such flags.
     */
    public final static boolean nextFlag=true;
    public final static boolean prevFlag=false;

    public boolean advanceToFlag(boolean whichFlag) throws Exception {

    ResultSet rs = H2SEvent.findNext(
                getRBB(PREDICTIONS).db(),
                getModel().getMinimalResultTags().toString(),
                getSimTime(),
                whichFlag,
                null);

        if(!rs.next()) {
            return false;
        }

        Tagset tags = new Tagset(rs.getString("TAGS"));

        selectInputs(tags);

        setSimTime(rs.getDouble("START_TIME"));
        return true;
    }

    public MLModel getModel() throws Exception {
        if(model==null)
            model = MLModel.open(getRBB(MODEL));
        return model;
    }

    public int getNumSelected() throws Exception {
        return RBBSelection.oneShot(getRBB(SESSION).db(), getRBB(COORDINATION).db()).getSelectedEventIDs().length;
    }

    public void deselectAll() throws Exception {
        RBBSelection.oneShot(getRBB(SESSION).db(), getRBB(COORDINATION).db()).deselectAll();
    }

    /**
     * Disconnect from any/all RBBs managed by this object.
     */
    public void disconnect() throws SQLException {
        for(RBB rbb : RBBs.values())
            rbb.disconnect();
        if(replayControl != null)
            replayControl.disconnect();
        RBBs.clear();
    }

    /**
     *
     * Retrieve currently selected Events constituting a problem instance that
     * conforms with the inputRequirements defined by the model, or with
     * the number of required inputs if inputRequirements are not defined.
     *
     * The inputRequirements defined by the model, if any, are enforced.
     */
     Event[] getSelectedProblemInstance(Mode op) throws Exception {

        Long[] selectedEventIDs = RBBSelection.oneShot(getRBB(SESSION).db(), getRBB(COORDINATION).db()).getSelectedEventIDs();
        Event[] selectedEvents = Event.getByIDs(getRBB(SESSION).db(), selectedEventIDs);
        Tagset[] require = getModel().getInputRequirements(op);
        if(require == null) {
            // we have no requirements to go by, so the number selected must equal the
            // number required and their order will be arbitrary.
            String[] inputNames = getModel().getInputNames(op);
            if(selectedEvents.length != inputNames.length)
                throw new Exception("Error: the model \""+getModel().getName()+"\" requires exactly "+inputNames.length+" selections for: "+StringsWriter.join(", ",inputNames)+" but there are "+selectedEventIDs.length+".");
        }
        else {
            Tagset[] tagsInOrder = Tagset.permutationIsSuperset(Event.getTagsets(selectedEvents), require);
            if(tagsInOrder == null)
                throw new Exception("Error: the selected data doesn't meet the requirements: "+StringsWriter.join("; ", require));
            // re-order the events so the right event (timeseries) goes to the right input.
            Map<Tagset, Event> tagsetToEvent = new HashMap<Tagset, Event>();
            for(Event e : selectedEvents)
                tagsetToEvent.put(e.getTagset(), e);
            selectedEvents = new Event[require.length];
            for(int i = 0; i < require.length; ++i)
                selectedEvents[i] = tagsetToEvent.get(tagsInOrder[i]);
        }
        return selectedEvents;
    }

    /**
     *
     * Given a tagset with tags whose names follow the format:
     * <input>.RBBID=<id>
     * For all training inputs of the specified model, select all the input events.
     *
     */
    public void selectInputs(Tagset t) throws Exception {
        deselectAll();
        String[] inputNames = getModel().getInputNames(PREDICTION, false);
        int i = 0;
        for(String inputName : inputNames) {
            final String tagName = inputName+".RBBID";
            final String value = t.getValue(tagName);
            if(value == null)
                throw new Exception("RBBML.selectInputs error: no tag found: \""+tagName+"\" in: " + t);
            RBBSelection.oneShot(getRBB(SESSION).db(), getRBB(COORDINATION).db()).selectEvents(byID(Long.parseLong(value)));
        }
    }


    public static class BadSelection extends Exception {
        public int numProblemInstancesSelected;
        public BadSelection(String msg, int numProblemInstancesSelected) {
            super(msg);
            this.numProblemInstancesSelected = numProblemInstancesSelected;
        }
    }


   /**
    * The groupTags array may be null.
    *
    * Create the tables for stored prediction parameters, if they didn't already exist.
    * It is OK if they already existed, since a single MLPredictions can contain stored parameters for any number of Sessions and Models.
    *
    * The table is only created on demand, because as Predictions RBB can be used without any stored Problem Sets.
    *
   **/
    public void createProblemSet(Tagset[] inputTags, Tagset[] groupTags, Tagset resultTags) throws Exception
    {
        if(groupTags==null)
            groupTags=new Tagset[0];
        if(inputTags.length != getModel().getInputNames(PREDICTION,false).length)
            throw new Exception("RBBML.createProblemSet called with "+inputTags.length+" tagsets, but "+getModel().getInputNames(PREDICTION,false).length+" tagsets (one for each input) are required.");
        if(groupTags.length != getModel().getInputNames(PREDICTION,true).length)
            throw new Exception("RBBML.createProblemSet called with "+groupTags.length+" group tagsets, but "+getModel().getInputNames(PREDICTION,true).length+" tagsets (one for each group) are required.");
        if(resultTags==null)
            throw new Exception("RBBML.createProblemSet called with null value for resultTags which is not allowed; instead pass an empty tagset");

        initPredictionsRBB();

        String q = "insert into RBBML_PROBLEMS values(nextval('RBB_ID'), ?, (";
        for(int i = 0; i < inputTags.length; ++i) {
           if(i > 0)
               q += ",";
           q += "RBB_TAGSET_TO_ID(?)";
        }
        q += "), (";
        for(int i = 0; i < groupTags.length; ++i) {
           if(i > 0)
               q += ",";
           q += "RBB_TAGSET_TO_ID(?)";
        }
        q += "), RBB_TAGSET_TO_ID(?));";

        java.sql.PreparedStatement stmt = getRBB(PREDICTIONS).db().prepareStatement(q);
        int iArg = 0;

        stmt.setString(++iArg, getModel().getName());

        for(int i = 0; i < inputTags.length; ++i)
           stmt.setString(++iArg, inputTags[i].toString());
        for(int i = 0; i < groupTags.length; ++i)
           stmt.setString(++iArg, groupTags[i].toString());

        stmt.setString(++iArg, resultTags.toString());

        // System.err.println("RBBML.createProblemSet: " + stmt.toString());
        stmt.execute();
   }

    /*
     * <pre>
     * Create a new Problem Set from the currently-selectedInputs events in the session.
     *
     * The number of selectedInputs events must equal the number of inputs for this getModel().
     *
     * Only one prediction input template is passed for all the parameters, because
     * it is not added if *any permutation* of it is a subset of an existing problem.
     * This implies order does not matter, so the template for all inputs must be the same.
     *
     * The template is completed for each input by
     * inheriting the value of the same tag from the Event with the tag selectedInputs=[iGroup+1]
     * (However, Tagset instance predictionInputTemplate itself will not be modified)
     *
     * predictionInputTemplate can also include Empty-string valued tags, which
     * means the value of the tag doesn't matter, but all inputs must have the same
     * value for it (consistent with MLOnline.batch and H2SEvent.findConcurrent())
     *
     * For example, in a 2-input model, if all the Events with a 'selectedInputs' tag are:
     * selectedInputs=1,type=airplane,session=monday,callsign=joe
     * selectedInputs=2,type=tank,session=tuesday,callsign=fred
     * ...and the predictionInputTemplate is:
     * type,session=
     * ...then the null-valued tags are filled in from the corresponding selectedInputs entity:
     * type=airplane,session=
     * type=tank,session=
     * ...note the value of the session tag was NOT inherited because it was not null, instead it was the empty string.
     * So when this problem set is evaluated, it will match reguardless of session, but after an airplane is found,
     * it will only be matched against tanks in the same session
     *
     * To prevent redundant predictions being run on problem instances,
     * The new problem set is NOT stored in the model if any permutation of the inputs
     * is a subset of the inputs of an existing stored problem set (even if the specified resultTags are different!)
     *
     * </pre>
     */
    public void createProblemSetFromSelectionIfNovel() throws Exception
    {
        final double now = getSimTime();

        Event[] inputs = getSelectedProblemInstance(PREDICTION);
        String[] inputNames = getModel().getInputNames(PREDICTION);
        ArrayList<Tagset> singleInputs = new ArrayList<Tagset>();
        ArrayList<Tagset> groupInputs = new ArrayList<Tagset>();

        // construct the candidate prediction input tagsets.
        Tagset[] template = getModel().getDefaultInputGeneralization();
        for (int i = 0; i < inputs.length; ++i) {
            Tagset t = template[i].clone();
            for(String tagName : template[i].getNames()) { // to prevent concurrentModificationException, iterate template[iGroup] but modify tags[iGroup]
                if(template[i].getValue(tagName) != null)
                    continue;
                final String newValue = inputs[i].getTagset().getValue(tagName);
                if(newValue==null)
                    throw new Exception("Error: one of the selected inputs does not have a tag named "+tagName+ " ("+inputs[i].getTagset().toString()+")");
                t.set(tagName, newValue);
            }
            if(getModel().isGroupInput(inputNames[i]))
                groupInputs.add(t);
            else
                singleInputs.add(t);
        }

        // Initialize the predictions tables in the predictions RBB, even though it will
        // raise an exception if no predictions rbb was specified.
        // There is no advantage to delaying initialization any further because
        // getProblemSets() will fail with an exception if the RBBML_PROBLEMS table has not been created.
        // If that is true, then we are about to create it in createProblemSet, so the database
        // definitely will be written to.
        initPredictionsRBB();

        // see if it is novel; if not, return immediately.
        for(ProblemSet ps : getProblemSets()) {
            // for now, assume each prediction input filter has 1 tagset.
            if(Tagset.permutationIsSuperset(ProblemSet.getTagsetFromEachFilter(ps.predictionInputFilter), singleInputs.toArray(new Tagset[0])) != null &&
               Tagset.permutationIsSuperset(ProblemSet.getTagsetFromEachFilter(ps.predictionGroupFilter), groupInputs.toArray(new Tagset[0])) != null)
                return;
        }

        createProblemSet(singleInputs.toArray(new Tagset[0]), groupInputs.toArray(new Tagset[0]), TC(""));
    }

    void initPredictionsRBB() throws Exception {
       java.sql.Statement sessionStmt = getRBB(PREDICTIONS).db().createStatement();
       sessionStmt.execute("create table if not exists RBBML_PROBLEMS (ID LONG, MODEL VARCHAR, INPUT_TAGS ARRAY, GROUP_TAGS ARRAY, RESULT_TAGS LONG);");
    }

    static String[] concat(String[] a, String[] b) {
        String[] result = new String[a.length+b.length];
        for(int i = 0; i < a.length; ++i)
                result[i] = a[i];
        for(int i = 0; i < b.length; ++i)
            result[i+a.length] = b[i];
        return result;
    }

    /**
     * Deletes the entire MLModel RBB *if* it's in a different RBB than the session itself.
     * The MODEL part must be available, since without that we don't even know its name.
     * <p>
     * If the PREDICTIONS part is available then the stored prediction parameters and predictions for this model are deleted, if any.
     * <p>
     * The SESSION part is not affected and need not be available.
     * <p>
     * The COORDINATION part is not affected and need not be available.
     * <p>
     * If any of the RBBs were not specified (iGroup.e. -rbb was not specified, and only some of -xxxRBB were)
     * then only those RBBs will be affected.
     * <p>
     * Finally, calls disconnect() so this RBBML instance has no open RBBs.
     */
    public void deleteModel() throws Exception {

        // delete stored prediction paramaters and predictions from PREDICTIONS part.
        if(wasPartSpecified(PREDICTIONS)) {
            deletePredictions();
            deleteProblemSets();
        }

        // delete the model
        // don't try to delete model if it's in the same RBB as the Session.
        // Todo: if not deleting the entire RBB, ought to at least drop the model-specific tables.
        if(!wasPartSpecified(SESSION) || !H2SRBB.getUUID(getRBB(MODEL).db()).equals(H2SRBB.getUUID(getRBB(SESSION).db()))) {
            System.err.println("deleting "+getURLForPart(MODEL));
            getRBB(MODEL).deleteRBB();
        }

        disconnect();
    }

    /**
     * Delete the predictions (data products) produced by this model.
     *
     * An example of deleting the predictions (and nothing else) is before making a new set of predictions when the model has been modified.
     */
    public int deletePredictions() throws Exception {
        return H2SEvent.delete(getRBB(PREDICTIONS).db(), byTags(getModel().getMinimalResultTags()));
    }

    /**
     * Delete the problem sets pertinent to this model.
     *
     * An example of deleting these (and nothing else) is in the AEMASE details dialog; when the user clicks OK
     * the code deletes all the old ones and stores the (possibly modified, or possibly the same) new ones.
     */
    public void deleteProblemSets() throws Exception {
        try {
            PreparedStatement prep = getRBB(PREDICTIONS).db().prepareStatement("delete from RBBML_PROBLEMS where MODEL = ?");
            prep.setString(1, getModel().getName());
            prep.execute();
        } catch(SQLException e) {
            if(!e.getSQLState().equals("42S02")) // this value means "Table "RBBML_PROBLEMS" not found" which is fine - there are no problem sets to delete.
                throw e;
        }
    }


    /**
     * Utility class representing one set of parameters for prediction (thus specifying a set of problem instances) -
     * corresponding to a row in RBBML_PROBLEMS
     */
    public static class ProblemSet {
        Long ID;
        String model;
        public RBBFilter[] predictionInputFilter;
        public RBBFilter[] predictionGroupFilter;
        public Tagset resultTags;

        @Override
        public String toString() {
            String s =
              "Problem set " + ID + " for model " + model +
              " (" + StringsWriter.join(", ", predictionInputFilter) +")" +
              " (" + StringsWriter.join(", ", predictionGroupFilter) +")";

            s += ") -> + (" + (resultTags==null?"null":resultTags.toString())+")";
            return s;
        }

        static Tagset[] getTagsetFromEachFilter(RBBFilter[] filters)
        {
            Tagset[] result = new Tagset[filters.length];
            for(int i = 0; i< result.length; ++i)
                result[i] = filters[i].tags[0];
            return result;
        }

    }


    /**
     * Find all problem sets defined on the model.
     *
     * Returns 0 if RBBML_PROBLEMS table has not been defined.
     */
    public ArrayList<ProblemSet> getProblemSets() throws Exception
    {
        try {
            String q = "select ID, MODEL, RBB_IDS_TO_TAGSETS(INPUT_TAGS) as INPUT_TAGS, RBB_IDS_TO_TAGSETS(GROUP_TAGS) as GROUP_TAGS, RBB_ID_TO_TAGSET(RESULT_TAGS) as RESULT_TAGS "
                    + "from RBBML_PROBLEMS where MODEL = ?";
        //
            // retrieve all problem sets for this scenario for which the flags are out-of-date.
            java.sql.PreparedStatement stmt = getRBB(PREDICTIONS).db().prepareStatement(q);

            stmt.setString(1, getModel().getName());

            ResultSet rs = stmt.executeQuery();

            ArrayList<ProblemSet> results = new ArrayList<ProblemSet>();

            while(rs.next())
            {
                ProblemSet ps = new ProblemSet();
                ps.ID = rs.getLong("ID");
                ps.model = rs.getString("MODEL");

                ps.predictionInputFilter = RBBFilter.fromStrings((Object[])rs.getArray("INPUT_TAGS").getArray());
                ps.predictionGroupFilter = RBBFilter.fromStrings((Object[])rs.getArray("GROUP_TAGS").getArray());

                Object[] resultTagsArray = (Object[]) rs.getArray("RESULT_TAGS").getArray();
                ps.resultTags = new Tagset(resultTagsArray);

                results.add(ps);
            }

            return results;
        } catch(SQLException e) {
            if(e.getSQLState().equals("42S02")) // table or view not found.
                return new ArrayList<ProblemSet>();
            else
                throw e;
        }
    }

    public Event[] getPredictions() throws Exception {
        return Event.find(getRBB(PREDICTIONS).db(), byTags(getModel().getMinimalResultTags()));
    }

    /*
     * For a given prediction result (such as from 'getPredictions'), retrieve the
     * Events that were the inputs for the prediction.
     */
    public Event[] getPredictionInputs(Event prediction) throws Exception {
        ArrayList<Long> inputIDs = new ArrayList<Long>();
        for(String inputName : getModel().getInputNames(PREDICTION, false))
            inputIDs.add(Long.parseLong(prediction.getTagset().getValue(inputName+".RBBID")));
        return Event.getByIDs(getRBB(SESSION).db(), inputIDs.toArray(new Long[0]));
    }

    /*
     * <pre>
     * Determine observation times for a problem instance.
     * All parameters are specified in the given time coordinate.  If it is null, no time conversions are performed.
     *
     * No times will be generated before the lowerBound or after the upperBound.
     *
     * TimeOfInterestStart and timeOfInterestEnd allow less than the whole duration of lowerBound/upperBound to be retrieved.
     * If specified they must lie within the lower/upper bounds.
     *
     * The warmup and cooldown periods are treated as 'best effort'; no exception is raised
     * if lowerBound + warmup > timeOfInterestStart.  (The rationale for this is that
     * feature extractor warmup/cooldown periods are often heuristic, and may actually
     * be based on something other than units of time in any case.)  However, when possible,
     * warmup/cooldown are exceeded rather than not being met.
     *
     * There are two basic cases, resampling vs. not resampling:
     * 1) Resampling is performed if resampleTimestep != null.  In this case no effort is
     *    made to use the actual observation times of the RBB data.  Samples will occur
     *    on multiples of resampleTimestep, or if timeOfInterestStart is specified, timeOfInterestStart+n*resampleTimestep.
     *    An exception is thrown if no observations occur, which means the timestep is bigger than the time of interest.
     *
     * 2) Resampling is not performed if resampleTimestep == null.  In this case
     *    only the observation times of timeseriesID are used.  The exception to this
     *    is that if timeseriesID was not observed between timeOfInterestStart and timeOfInterestEnd (and they are not null),
     *    then 1 observation at timeOfInterestStart is added.  (The rationale for this
     *    is that asking for a specific time is often used to interpolate a value for that
     *    specific moment, and it is surprising when no observation can be generated in the middle of a problem!)
     *
     * <pre>
     */
    public static Double[] makeObservationTimes(RBB rbb, final Long timeseriesID,
            final double lowerBound, final double upperBound,
            final Double timeOfInterestStart, final Double timeOfInterestEnd,
            final Double warmup, final Double cooldown,
            final Double resampleTimestep,
            String timeCoordinate, H2STime.Cache timeCache) throws Exception {

        // check parameters
        if(lowerBound > upperBound)
            throw new Exception("makeObservationTimes: lowerBound > upperBound: "+lowerBound+" >= "+upperBound);
        if(timeOfInterestStart != null && timeOfInterestStart < lowerBound)
            throw new Exception("makeObservationTimes: timeOfInterestStart < lowerBound: "+timeOfInterestStart+" < "+lowerBound);
        if(timeOfInterestEnd != null && timeOfInterestStart != null && timeOfInterestEnd < timeOfInterestStart)
            throw new Exception("makeObservationTimes: timeOfInterestEnd < timeOfInterestStart: "+timeOfInterestEnd+" < "+timeOfInterestStart);
        if(timeOfInterestEnd != null && timeOfInterestEnd < lowerBound)
            throw new Exception("makeObservationTimes: timeOfInterestEnd < lowerBound: "+timeOfInterestEnd+" < "+lowerBound);
        if(timeOfInterestEnd != null && timeOfInterestEnd > upperBound)
            throw new Exception("makeObservationTimes: timeOfInterestEnd > upperBound: "+timeOfInterestEnd+" > "+upperBound);

//        if(timeCache == null)
//            timeCache = new H2STime.Cache();

         ArrayList<Double> result = new ArrayList<Double>();

        // the warmup and cooldown times are not yet in the required time coordinate.
        // For example, if the implicit time coordinate of the RBB is in seconds but
        // the specified timeCoordinate is in minutes, the warmup and cooldown times


        Double start = lowerBound;
        if(timeOfInterestStart != null && warmup != null) // there is a limit (other than lowerBound) on how early we want/need to start observing
            start = Math.max(lowerBound, timeOfInterestStart - warmup);

        Double end = upperBound;
        if(timeOfInterestEnd != null && cooldown != null) // there is a limit (other than upperBound) of how late we want/need to stop observing.
            end = Math.min(upperBound, timeOfInterestEnd + cooldown);

        if(resampleTimestep == null) { // use the observation times of timeseriesID

            // if a warmup or cooldown is required and the times of existing samples are
            // used, get one extra observation before
            // or after those within the time of interest plus the warmup/cooldown time.
            // Otherwise we would be rounding down the warmup time, instead of up,
            // so feature extractors would not get the full warmup time.
            // On the other hand, for the simple case of getting a single observation
            // at a specific time with no warmup time, there's no reason to get any extra samples.
            final int numWarmup = warmup != null && warmup > 0 ? 1 : 0;
            final int numCooldown = cooldown != null && cooldown > 0 ? 1 : 0;

            ResultSet rs = H2STimeseries.getSamples(rbb.db(), timeseriesID, start, end, numWarmup, numCooldown,
                    timeCoordinate, timeCache);

            int numDuringTimeOfInterest = 0;

            while(rs.next()) {
                double t = rs.getDouble(1);
                if(t < lowerBound || t > upperBound)
                    continue;

                // we only need at most one observation at or before 'start', but could wind up
                // with 2 due to numWarmup.
                if(t <= start && !result.isEmpty() && result.get(result.size()-1) <= start)
                    result.remove(result.size()-1);

                // special-case to make sure we get an observation during time of interest.
                if(timeOfInterestStart != null && timeOfInterestEnd != null) {
                    if(t >= timeOfInterestStart && t <= timeOfInterestEnd)
                        ++numDuringTimeOfInterest;
                    else if(t > timeOfInterestEnd && numDuringTimeOfInterest == 0) {
                        result.add(timeOfInterestStart);
                        ++numDuringTimeOfInterest;
                    }
                }
                result.add(t);
            }

            // continuation of special case to make sure we get an observation during time of interest.
            if(timeOfInterestStart != null && timeOfInterestEnd != null &&
                    numDuringTimeOfInterest == 0)
                result.add(timeOfInterestStart);
        }
        else { //// with resampling.
            if(timeOfInterestStart != null) 
                start = timeOfInterestStart - resampleTimestep * Math.min(
                        Math.ceil((timeOfInterestStart-start) / resampleTimestep), // this is the min number of timesteps that will meet or exceed warmup
                        Math.floor((timeOfInterestStart-lowerBound) / resampleTimestep)); // this is max timesteps possible before undershooting minBound
            else // advance to next multiple of resampleTimestep
                start = resampleTimestep * Math.ceil(start / resampleTimestep);

            int n = (int) Math.floor((end-start)/resampleTimestep);
            for(int i = 0; i <= n; ++i)
                result.add(start + resampleTimestep * i);

            if(result.size() == 0)
                throw new Exception("makeObservtionTimes Error: no samples will be generated... the specified timestep is larger than the time period of interest.");
        }

        return result.toArray(new Double[0]);
    }

    /**
     *
     * This function applies all the ProblemSets and stores the resulting
     * observations in problemData.
     * If applying all the ProblemSets does not result in exactly 1
     * problem instance, BadSelection is thrown.  It is up to the application
     * to determine what was wrong with the selection:

     * 1) There were no defined ProblemSets.  If this is the only problem with the
     * selectedInputs data, the application should probably use some heuristic or user
     * interface to create a ProblemSet from the current data selection and
     * then invoke this function again.
     *
     * 2) Too few or too many Events selectedInputs.  This results in zero or multiple
     * Problem Instances being submitted as training examples, which is presumed
     * (by this function only) to be a mistake and so not supported.
     *
     * 3) Mismatching data types selectedInputs.  For example, if there is one ProblemSet
     * defined on the tagsets "color=red" and "color=blue") but two Events both
     * with "color=red" are selectedInputs, then there will be no problem instances
     * generated.  In this case the application could define a new Problem Set
     * covering the selectedInputs data, perhaps after asking the user whether this
     * was intentional.
     *
     *
     */


    /**
     *
     * Apply the given feature extractor chain to the currently selected inputs.
     *
     * Throws an informative error message if the selected inputs are not valid.
     *
     * For detail on the times of the observations see makeObservationTimes()
     *
     */
    public void observeSelectedTrainingExample(MLFeatureExtractor sink) throws Exception {

        Double stop = getSimTime();
        Double start = stop - getHistoryLength();

        Event[] problemInstance = getSelectedProblemInstance(TRAINING);

        // Get the latest starting and first ending times of the selectedInputs events.
        for(Event event : problemInstance) {
            if(event.getStart() > start)
                start = event.getStart();
            if(event.getEnd() < stop)
                stop = event.getEnd();
        }

        if(start > stop)
            throw new Exception("RBBML.observeSelectedTrainingExample Error: the selected Events do not co-occur during the selected period of time.");

        MLFeatureExtractor fe = getModel().getTrainingFE();

        Double[] observationTimes = makeObservationTimes(getRBB(SESSION), problemInstance[0].getID(),
                start, stop,
                start, stop,
                fe.getMaxWarmup(), fe.getMaxCooldown(),
                getModel().getTimestep(), null, null);

        fe.addToChain(
            new ObserveTimespanFE(start, stop,
            new ProblemAgeFE("Training_Event_Timing", sink)));
        
        batchInstance(
            getModel().getInputNames(TRAINING, false),
            getModel().getInputNames(TRAINING, true), new RBBFilter[0], // todo: doesn't yet support groups.
            Event.getIDs(problemInstance),
            fe, start, stop, observationTimes, null);

    }

    public void addSelectedTrainingData(String label) throws Exception {
        ArrayList<MLObservationSequence> obs = new ArrayList<MLObservationSequence>();
        observeSelectedTrainingExample(new BufferObservationsFE(obs, null));
        // if an incorrect data selection were made, then observeSelectedTrainingExamples would have
        // raised an exception itself.  If obs.size()==0, then a problem instance existed
        // but didn't create any observations.
        // It could be that there wasn't enough history prior to the start time for the warmup period.
        // Try a later 'start' value, or increase the model sample rate (MLModel.getSampleRate())
        if(obs.size() != 1) // check that one observation sequence (note: may contain multiple observations) was created.
            throw new Exception("Error: no observations were generated.  Try advancing the time by a second or two, and try again.");
        getModel().addTrainingData(getRBB(COORDINATION), obs.get(0), label);
    }

    /**
     * The AEMASE front-end displays the result of toString() in the model
     * selection dialog, because it adds the RBBML instance to the combo box:
     * So, changing this to something else has a user-visible effect.
     *
     * cboModel.addElement(aemaseSpatial);
     */
    @Override
    public String toString() {
        try {
            return getModel().getName();
        }
        catch(Exception e)
        {
            return null;
        }
    }



    /**
     *
     * A problem definition specifies a type of "problem", which is a co-occurrence
     * of events that match a pattern (an array of tagsets)
     *
     * The problem definition monitors Events as they are created and detects
     * specified co-occurrences which are instantiated as Problem Instances.
     *
     * The Events whose co-occurrence constitutes a problem instance are called
     * 'individuals' because each unique combination constitutes a different problem set.
     * A problem definition may also involve 'groups' of events.  A group is a set
     * of Events matching a tagset, but they are treated collectively as a single
     * feature in an Observation (albeit an array, but with only one name) and
     * the number of Events in a group can vary over time during a single problem
     * instance, and can even be 0.
     *
     * The inputs defining the problem may have cross-references ("join")
     *
     * For example, say we want to watch for
     * situations where an aircraft is low on fuel relative to its distance from the
     * carrier.  A problem instance consists of the position of a plane, and the
     * fuel load of that same plane.  Assuming each entity is uniquely identified
     * by a callsign, this problem is specified with the inputTags:
     * side=friendly,domain=air,variable=position callsign,variable=fuel
     * findConcurrentEvents resolves the missing 'callsign' value by binding it
     * to whatever value is found in the callsign tag of the tagset to the left.
     *
     * The triggers for ProblemEvaluator are:
     * init: due to creation of an Individual Event, a new combination of Individuals matching
     *   a MLProblemDefinition now exists, so a new ProblemInstance is created.
     *   (TODO: this should also occur if inputTags are added to an Event so it now permutationIsSubset)
     * observe: obs was added to any obs table associated with an Event.
     *   In most cases this means a row was added to a Timeseries table.
     * done: ProblemEvaluator.done() is called when:
     *   1. an end time is set for any of the contributing Events
     *   2. any of the related events is deleted (TODO: not implemented)
     *
     * Rationale for obs structures implementing this class:
     * The most common operation is expected to be eventDataAdded, which is triggered
     * whenever an observation is added to any of the timeSeries in the problem.
     * So, lookup from an Event ID to all matching problem instances should be fast.
     *
     * EventID -> ProblemInstances
     *
     * Another frequent operation is checking whether a problem instance already
     * exists.  For that we keep a set of lists of EventIDs.
     *
     * EventIDs
     *
     * When an Event ends, all the associated ProblemInstances must be done(),
     * and all the other events referencing the probleminstance must be unlinked.
     *
     * @author rgabbot
     */
    public ResultSet batch(
        String[] inputNames,
        RBBFilter[] inputFilters,
        String[] groupNames,
        RBBFilter[] groupFilters,
        Object[] skip,
        MLFeatureExtractor fe,
        Double start,
        Double end,
        Double timestep,
        String timeCoordinate)
        throws Exception
    {
        org.h2.tools.SimpleResultSet result = new org.h2.tools.SimpleResultSet();
        result.addColumn("START_TIME", java.sql.Types.DOUBLE, 20, 0);
        result.addColumn("END_TIME", java.sql.Types.DOUBLE, 20, 0);
        result.addColumn("IDS", java.sql.Types.ARRAY, 20, 0);

        RBB inputRBB = getRBB(MLPart.SESSION);

        if (inputRBB.db().getMetaData().getURL().equals("jdbc:columnlist:connection"))
            return result;

        ResultSet rs = H2SEvent.findConcurrent(inputRBB.db(), inputFilters, skip, start, end,
                timeCoordinate, null);

        while(rs.next())
        try {
            Double problemStartTime = rs.getDouble("START_TIME");
            if(start != null && problemStartTime < start)
                problemStartTime = start;

            Double problemEndTime = rs.getDouble("END_TIME");
            if(end != null && problemEndTime > end)
                problemEndTime = end;

            Long[] eventIDs = (Long[]) rs.getArray("IDS").getArray();

            Double[] observationTimes = RBBML.makeObservationTimes(inputRBB, eventIDs[0], problemStartTime, problemEndTime, start, end,
                    fe.getMaxWarmup(), fe.getMaxCooldown(), timestep, timeCoordinate, null);

            // todo: tags for groups with empty-string values should have that
            // value taken from the nearest preceeding group or individual input,
            // as is done with individual inputs.
            batchInstance(inputNames, groupNames, groupFilters,
                eventIDs, fe, problemStartTime, problemEndTime, observationTimes, timeCoordinate);

            // the problem instance was executed with no exception, so report it to the caller.
            result.addRow(problemStartTime, problemEndTime, rs.getObject("IDS"));

//        } catch(ProblemSpecificException e) {
//            // don't add to results, but keep executing problem instances.
//            System.err.println(e.getMessage());
        } catch(SQLException e) {
            if(e.getSQLState() != null && e.getSQLState().equals(H2STimeseries.RBB_EMPTY_TIMESERIES)) {
                System.err.println("MLBatch: " + e.getMessage());
                // this exception affects only this problem set and doesn't mean other problem sets will fail, so continue.
            }
            else {
               throw e;
            }
        }
        // could do rs.beforeFirst() and return to caller, but beforeFirst() doesn'start0 seem to do anything.
        // rs.beforeFirst();

        fe.batchDone();

        return result;
    }

        /*
         * A ProblemSpecificException is one that effects only this problem instance,
         * not a more general problem like SQL Connection broken.
         */
        public static class ProblemSpecificException extends Exception {
            ProblemSpecificException(Long[] eventIDs, String s) {
                super("MLBatch.batchProblemInstance: skipping problem set ("+StringsWriter.join(",", eventIDs) + "): "+s);
            }
        }

    /**
     * Batch evaluate a single problem instance, specified by a set of eventIDs
     *
     * @param inputRBB
     * @param outputRBB
     * @param inputNames
     * @param groupNames
     * @param groupTags
     * @param eventIDs
     * @param fe
     * @param problemStartTime
     * @param problemEndTime
     * @param timestep
     * @throws Exception:  If the error is problem-specific, an instance of ProblemSpecificInstance will be thrown.
     */
    public void batchInstance(
        String[] inputNames,
        String[] groupNames,
        RBBFilter[] groupFilters,
        Long[] eventIDs,
        MLFeatureExtractor fe,
        double problemStartTime,
        double problemEndTime,
        Double [] observationTimes,
        String timeCoordinate) throws Exception {



        class Group extends ArrayList<Timeseries> {
            MLObservation.Metadata metadata;

            Group(String groupName, RBBFilter groupFilter, Double[] times, String timeCoordinate) throws Exception {
                // load up the initial set of members.  This will be updated if/when subsequent events are created.
                //                this.addAll(Arrays.asList(getRBB(SESSION).findTimeseries(groupTags, times[0], times[times.length-1], timeCoordinate)));
//                addAll(Timeseries.get(getRBB(SESSION), groupTags, times[0], times[times.length - 1], null, timeCoordinate));
                RBBFilter f = new RBBFilter(groupFilter, byTime(times[0], times[times.length - 1]));
                for(Timeseries t : Timeseries.findWithSamples(getRBB(SESSION).db(), f , withTimeCoordinate(timeCoordinate)))
                    super.add(t);
                if(isEmpty())
                    System.err.println("RBBML.batchInstance.Group warning - no entities match  " + f + "; group will always be empty!");
            }
            
            /*
             * Set the Group feature for this group in the specified Observation
             */
            MLObservation.GroupFeature observe(double t) {
                if(!isMetadataCorrect(t))
                    metadata = getMetadata(t);
                MLObservation.GroupFeature obs = new MLObservation.GroupFeature(t, metadata);
                for(Timeseries ts : this)
                    try {
                        obs.setFeature(ts.getID().toString(), ts.valueLinear(t));
                    } catch(Exception e) {
                    }
                return obs;
            }

            private boolean isMetadataCorrect(double t) {
                if(metadata == null)
                    return false;
                int numPresent = 0; // number of group members with non-null observed value at timeStep
                for(Timeseries ts : this) {
                    if(ts.valueLinear(t) == null)
                        continue;
                    ++numPresent;
                    if(!metadata.getFeatureNames().contains(ts.getID().toString()))
                        return false;
                }
                // since we now know the metadata contains at least all the name of members currently present,
                // if it is the same size it must match exactly.
                if(metadata.getFeatureNames().size() != numPresent)
                    return false;
                return true;
            }

            /**
             * Creates a new metadata correct for the specified timestep
             */
            private MLObservation.Metadata getMetadata(double t) {
                MLObservation.Metadata md = new MLObservation.Metadata();
                for(Timeseries ts : this) {
                    if(ts.valueLinear(t) != null)
                        md.add(ts.getID().toString(), ts);
                }
                return md;
            }
        }

        if(eventIDs.length != inputNames.length)
            throw new Exception("ProblemDefinition.batch - the number of input tagsets doesn't match the number of input names!");

        // Make sure all the Events are timeseries.
        //
        // It's easy to break this rule using
        // predict -storeEvents
        // because the events inherit inputTags from the inputs, and can be therefore
        // be mistakenly used as inputs for new problem instances.
        //
        // If this happens, you probably need to use
        // predict -resultTags
        // so results have inputTags to distinguish them from inputs.
        Event[] ev = Event.getByIDs(getRBB(SESSION).db(), eventIDs, withTimeCoordinate(timeCoordinate));

//        ResultSet samples = H2STimeseries.resampleValues(getRBB(SESSION).db(), eventIDs, observationTimes, timeCoordinate==null?null:timeCoordinate.toArray());
        ResultSet samples = H2STimeseries.resampleValues(getRBB(SESSION).db(), eventIDs, observationTimes, timeCoordinate);

        // construct metadata for observations
        MLObservation.Metadata md = new MLObservation.Metadata();
        for(int i = 0; i < inputNames.length; ++i)
            md.add(inputNames[i], ev[i]);
        for(int i = 0; groupNames != null && i < groupNames.length; ++i)
            md.add(groupNames[i], null);
        fe.addSelfToFeatureNames(md);

        fe.setRBBML(this);

        // use the problem start time,
        // which may be before observationTimes[0]
        fe.init(problemStartTime, md);

        // find all the members of each group input that will be observed sometime during this problem instance.
        if(groupNames==null)
            groupNames = new String[0];
        // for each group tag that has an empty value, set the value to the value from the same-named tag in the most recently preceeding input.
        // (a group cannot inherit tag values from another group)
//        for(int iGroup = 0; iGroup < groupNames.length; ++iGroup)
//            TAGNAMES: for(String name : groupTags[iGroup].getNames())
//                for(String value : groupTags[iGroup].getValues(name))
//                    if("".equals(value))
//                        for(int iInput=ev.length-1; iInput >= 0; --iInput)
//                            for(String inputValue : ev[iInput].getTagset().getValues(name)) {
//                                groupTags = groupTags.clone();
//                                groupTags[iGroup] = groupTags[iGroup].clone();
//                                groupTags[iGroup].set(name, inputValue);
//                                // System.err.println("Group inherited tag value "+name+"="+inputValue);
//                                continue TAGNAMES;
//                            }
        Group[] groups = new Group[groupNames.length];
        for(int iGroup = 0; iGroup < groupNames.length; ++iGroup)
            groups[iGroup] = new Group(groupNames[iGroup], groupFilters[iGroup], observationTimes, timeCoordinate);

        for(double t0 : observationTimes) {
            samples.next();
            // build the current observation using the next interpolated value for each timeseries.
            MLObservation obs = new MLObservation(t0, md);
            for(int i = 0; i < inputNames.length; ++i) {
                java.sql.Array a = samples.getArray(i+2);
                if(a != null)
                    obs.setFeature(inputNames[i], (Object[])a.getArray());// +2 because first col is time and getArray(n) is 1-based
            }

            for(int iGroup = 0; iGroup < groups.length; ++iGroup)
                obs.setFeature(groupNames[iGroup], groups[iGroup].observe(t0));

            fe.observe(obs);
        }

        fe.done(problemEndTime);
    }

}



