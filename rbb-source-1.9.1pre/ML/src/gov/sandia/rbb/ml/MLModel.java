package gov.sandia.rbb.ml;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

import gov.sandia.rbb.Timeseries;
import gov.sandia.rbb.ml.features.CreateTimeseriesFE;
import gov.sandia.rbb.util.StringsWriter;
import gov.sandia.rbb.impl.h2.statics.H2SEvent;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.ArrayList;
import gov.sandia.rbb.Tagset;
import java.sql.ResultSet;
import java.io.StringWriter;
import gov.sandia.rbb.Event;
import gov.sandia.rbb.RBB;
import java.sql.SQLException;
import gov.sandia.rbb.ml.RBBML.Mode;
import static gov.sandia.rbb.ml.RBBML.Mode.*;
import static gov.sandia.rbb.RBBFilter.*;

/**
 * An RBB ML Model is an RBB populated with the necessary information to
 * create and parameterize a predictive model.
 *
 * This is the base class for overrides that implement specific models.
 *
 * @author rgabbot
 */
abstract public class MLModel {

    private RBB rbb;

    /**
     *
     * This function must return a feature extraction chain.
     * Its input is Observations with only the Prediction Input features (getInputNames(PREDICTION))
     * The Output is Observations that include the predicted feature.
     *
     * However, the chain should not have any side effects on the RBB.
     * This is done by the feature extraction chain from getActionFE() which is
     * appended when necessary.
     *
     */
    public abstract MLFeatureExtractor getPredictionFE() throws Exception;

    /**
     * 
     * The "action" is what the application does with the prediction,
     * such as creating flags or controlling an entity.
     * 
     * The default action is to create a timeseries from the prediction (getPredictionName())
     * using CreateTimeseriesFE with the tagset specified by getResultTags()
     * 
     */
    public MLFeatureExtractor getActionFE() throws Exception {
        return new CreateTimeseriesFE(getResultTags(), getPredictionName(), false, true, null);
    }


    /**
     * This function must return a feature extraction chain.
     * Its input is Observations with only the Training Input features.
     * The output is Observations ready to be added as training examples.
     * (i.e. with derived features, and a feature that is a prediction/score. etc).
     *
     * However, this feature extraction chain should stop short of any
     * side effects, such as storing the training examples.
     *
     * parseTrainingArgs() will always be called before getTrainingFE is
     * used to create a chain that will actually be used to create training
     * data, however parseTrainingArgs is not called before using getTrainingFE
     * to extract features for plotting or printing out.
     *
     */
    public abstract MLFeatureExtractor getTrainingFE() throws Exception;

    protected RBB getRBB() {
        return rbb;
    }

    /**
     * Create a new MLModel in an already-open RBB.
     * There cannot already be a model in the RBB, because an RBB can only hold one Model.
     */
    static public MLModel create(RBB modelRBB, String className, String user) throws Exception {

//        final String className = this.getClass().getCanonicalName();

        MLModel model = instantiateModel(className);
        model.rbb = modelRBB;

//        if(className==null)
//            throw new SQLException("Error creating model " + modelName + ": getClass().getCanonicalName() returned null.  The RBB ML Model class cannot be a local or anonymous class.");

        StringWriter q = new StringWriter();

        q.write("create table RBBML_DESCRIPTOR (RBBML_SCHEMA_VERSION INT, RBB_ML_MODEL_CLASS VARCHAR);");
        q.write("insert into RBBML_DESCRIPTOR values ("+codeSchemaVersion()+", '"+className+"');");

        q.write("create table RBBML_TRAINING_DATA (TRAINING_EVENT BIGINT, WEIGHT REAL");
        for(String name : model.getDefaultDBFeatureNames()) {
            q.write(", \""); // the quotes around the name make it case-sensitive.
            q.write(name);
            q.write("\" ARRAY");
        }
        q.write(");");

        // System.err.println(q.toString());

        modelRBB.db().createStatement().execute(q.toString());

        model.setPredictorNames(model.getDefaultPredictorNames());

        System.err.println("Created Model '"+modelRBB.getName()+"'");

        return model;
    }

    /**
     * Allocate and open an MLModel instance on an already-open RBB
     * on which create() has already been called.
     *
     *
     * the Java type information in the RBBML_DESCRIPTOR table.
     */
    static public MLModel open(RBB modelRBB) throws Exception {

        // this statement will throw a SQLException if RBBML_DESCRIPTOR is not in the modelURL RBB.
        ResultSet rs;
        try{ rs=modelRBB.db().createStatement().executeQuery("select RBB_ML_MODEL_CLASS, RBBML_SCHEMA_VERSION from RBBML_DESCRIPTOR"); }
        catch(Exception e) { throw new SQLException("Opened RBB ML model but then got error retrieving RBB_ML_MODEL_CLASS: " + e.getMessage()); }
        if(!rs.next())
            throw new SQLException("RBBML_DESCRIPTOR table in model DB is empty!");
        final String className = rs.getString("RBB_ML_MODEL_CLASS");
        final int dbSchemaVersion = rs.getInt("RBBML_SCHEMA_VERSION");

        if(dbSchemaVersion != codeSchemaVersion())
            throw new SQLException("MLModel.open: the model is an outdated version: " +
                    dbSchemaVersion + ".  The current version is: " + codeSchemaVersion() + ".\n");

        MLModel model = instantiateModel(className);
        model.rbb = modelRBB;

        return model;
    }

    /**
     * Create an un-initialized MLModel instance from the specified className,
     * which must be a class that extends MLModel.
     *
     * This function doesn't return null; it throws an exception if it cannot instantiate.
     *
     */
     static MLModel instantiateModel(String className) throws Exception {
        try {
            Class c = Class.forName(className);
            if(c==null)
                throw new Exception("MLModel failed to get class "+className+" The RBB ML Model class cannot be a local or anonymous class.");
            MLModel a = (MLModel) c.newInstance();
            if(a==null)
                throw new Exception("MLModel failed to instantiate "+className);
           return a;

        } catch (Exception e) {
            throw new Exception("MLModel failed to instantiate " + className +"; is it in the java classpath?  Caught: "+e.toString());
        }
    }

     /**
      * "Inputs" here refers to the initial inputs to this model,
      * before any feature extraction takes place.
      * Inputs (by definition) come through the RBB rather than being created by
      * feature extractors.
      *<p>
      * The names should imply the meanings of the inputs.  E.g. for a model
      * whose inputs are the positions of a pair of entities, the input feature
      * names could be new String[]{ "PositionA", "PositionB" }
      *<p>
      * The Mode is passed in case the model requires different inputs for training
      * vs prediction.  However, the Prediction Inputs
      * MUST be a subset of the Training Inputs
      * i.e. the Training environment is "richer" than the Prediction environment.
    */
    public abstract String[] getInputNames(Mode m);

    /**
     * If the model uses any group inputs, 
     * this must be overridden to return true for the names of any 
     * inputs to be treated as groups.
     *
     * Defaults to false since many models don't use group inputs.
     *
     * Design rationale:
     * In the code, most callers either need the individual inputs, or group
     * inputs, but not both in a single array, as getInputNames returns.
     * The main reason for returning them in a single array is to make MLModel-derived
     * classes more easy to understand and implement, because the methods
     * getDefaultinputGeneralization() and getInputRequirements() return
     * arrays that are parallel with the (entire list of) inputs.
     *
     */
    public boolean isGroupInput(String inputName) {
        return false;
    }

    
    /**
     * <pre>
     * For each input, specifies the minimal tags required to be a valid input.
     *
     * The default is to return null, in which case no requirement is applied.
     * This is fine if the application doesn't offer the user a way to make
     * invalid selections and the selection order does not matter.
     *
     * The purpose of imposing requirements is mainly to assist the heuristics
     * that decide how to create training data and stored prediction parameters
     * from selected data:
     *
     * - Selected data not meeting the requirements can be ignored, such as when
     *   multiple timeseries representing a single entity are selected together, but
     *   only (e.g.) the position timeseries is a valid input.
     * - An ordering can be placed on inputs.  For example if a model semantics
     *   assume the first input is a position and the second a velocity, but
     *   the user clicks on an entity and it selects the velocity first and then
     *   the position, this allows the inputs to be re-ordered.
     * - Listing a tag name without a value implies the tag is required, and also
     *   indicates to the UI that the user should be offered a choice among the
     *   tag values, or "Any" (i.e. null).
     *
     * </pre>
     *
     */
    public Tagset[] getInputRequirements(Mode m) {
        return null;
    }

    /**
     *<pre>
     * It is not necessary to override this unless the application creates
     * Problem Sets from selected data added as training examples by
     * calling RBBML.createProblemSetFromSelectionIfNovel()
     *
     * When data is selected and added to the model as training data,
     * RBBML assumes the user wants to apply the model to other, similar
     * problem instances.  This function determines what "similar" is by default.
     *
     * For example if a timeseries used as a training example has the tagset
     * "color=blue,id=4"
     * This function would return new Tagset[]{ TC("color") }
     * with the result that the model will be applied to all timeseries
     * with the tags color=blue.
     *</pre>
     */
    public Tagset[] getDefaultInputGeneralization() {
       return getInputRequirements(TRAINING);
    }

    /**
     *
     * This form of getInputNames is for when the code needs only group inputs (true)
     * or only individual (non-group) inputs (false)
     *
     */
    public String[] getInputNames(Mode op, boolean groups) {
        int n = 0;
        final String[] allInputNames = getInputNames(op);
        for(String inputName : allInputNames)
            if(isGroupInput(inputName) == groups)
                ++n;
        if(n == allInputNames.length)
            return allInputNames;
        if(n==0)
            return new String[0];

        String[] results = new String[n];
        int i = 0;
        for(String inputName : allInputNames)
            if(isGroupInput(inputName) == groups)
                results[i++] = inputName;

        return results;
    }

//     /**
//      * Get the names of group inputs for training examples.
//      * By default returns the empty array because not all models use group inputs.
//      */
//     public String[] getGroupInputNames(MLOpEnum m) {
//        return new String[0];
//     }

     /**
      * Accept any model-specific configuration required for training.
      *
      * Should throw a descriptive error message and/or usage string if necessary.
      *
      */
     public void parseTrainingArgs(String... args) throws Exception {
         if(args.length > 0)
             throw new Exception("No model-specific training args expected, but got " + StringsWriter.join(" ", args));
     }

     /**
      * Accept any configuration required for prediction.
      * Should throw a descriptive error message and/or usage string if necessary.
      */
     public void parsePredictionArgs(String... args) throws Exception {
         if(args.length > 0)
             throw new Exception("No model-specific prediction args expected, but got " + StringsWriter.join(" ", args));
     }

    /**
     * RBBML_TRAINING_DATA.WEIGHTS specifies a weight for each example.
     * In general these may be set and applied by different algorithms in different ways, except that
     * only examples with weight > 0 will be presented to the learning algorithm at all.
     *
     * The meanings of these constants:
     *  DEFAULT = weight for new examples
     *  DELETED = weight assigned for examples that are 'deleted'
     *
     */
    final float TRAINING_EXAMPLE_WEIGHT__DEFAULT = 1.0f;
    final float TRAINING_EXAMPLE_WEIGHT__DELETED = -1.0f;

    /**
     * Get the name of the feature being predicted - i.e. the output of the machine learning algorithm.
     *
     * By default this is the output of the last feature extractor in the getTrainingFE chain
     * that does not start with underscore, (or the last input that doesn't start with underscore
     * if there is no such feature extractor).
     *
     */
    public String getPredictionName() throws Exception {
        String result = null;
        for(String inputName : getInputNames(TRAINING))
            if(!inputName.startsWith("_"))
                result = inputName;
        MLFeatureExtractor fe = getTrainingFE();
        while(fe != null) {
            for(int i = 0; i < fe.getNumOutputs(); ++i)
                if(!fe.getOutputName(i).startsWith("_"))
                    result = fe.getOutputName(i);
            fe = fe.nextFeatureExtractor;
        }

        if(result == null)
            throw new Exception("MLModel.getPredictionName() error: all the inputs and derived features start with underscore.  Rename one or override this method.");
        else
            return result;
    }

//    public int getNumInputs(MLOpEnum m) {
//        return getInputNames(m).length;
//    }

//    public int getNumGroupInputs(MLOpEnum m) {
//        return getGroupInputNames(m).length;
//    }

    /**
     * Name of the model as a whole.
     */
    public String getName() throws Exception {
        return rbb.getName();
    }

    /**
     * Get the names of features in the training examples to store in the database
     * for possible later use in training the model (either as a predictor or the prediction)
     *
     * This default implementation returns the names of all training inputs and
     * derived features that do NOT start with underscore.
     *
     * This function returns the "default" feature names because this function is only called
     * once, during create().  After that the names of features are always read from the database.
     * So it is allowable, for example, to add a new Feature to an existing Model after it
     * is created, even though getDBFeatureNames() will then return a superset of
     * what this function returns.
     *
     */
     public String[] getDefaultDBFeatureNames() throws Exception {
         ArrayList<String> result = new ArrayList<String>();

        for(String inputName : getInputNames(TRAINING))
            if(!inputName.startsWith("_"))
                result.add(inputName);

        MLFeatureExtractor fe = getTrainingFE();
        while(fe != null) {
            for(int i = 0; i < fe.getNumOutputs(); ++i)
                if(!fe.getOutputName(i).startsWith("_"))
                    result.add(fe.getOutputName(i));
            fe = fe.nextFeatureExtractor;
        }

        // System.err.println("getDefaultDBFeatureNames returning: " + result.toString());

         return result.toArray(new String[0]);
     }

    public String[] getDBFeatureNames() throws Exception {
        ArrayList<String> result = new ArrayList<String>();
        ResultSet rs = rbb.db().createStatement().executeQuery("select column_name from information_schema.columns where table_name = 'RBBML_TRAINING_DATA' order by ordinal_position");
//            System.err.println("getDBFeatureNames:");
        for(int i = 0; rs.next(); ++i) {
//                System.err.println("\t"+rs.getString(1));
            if(i >= 2) // first two cols are not features (TRAINING_EVENT and WEIGHT)
                result.add(rs.getString(1));
        }
        return result.toArray(new String[0]);
    }

    /**
     *
     * Specifies which features to use as independent variables in the learner.
     * <p>
     * Of all the features stored in the DB (i.e. those returned by getDBFeatureNames()),
     * only the subset returned by getPredictorNames() are actually to be used by the prediction algorithm.
     * (The rest are stored in case they are later determined to be useful.)
     * <p>
     * The default set of predictors is determined by getDefaultPredictorNames(),
     * which can be overridden if necessary.
     *
     */
    public String[] getPredictorNames() throws Exception {
        Event[] featureSelectionEvents = Event.find(rbb.db(), byTags("type=RBBML_Feature_Selection"));
        if(featureSelectionEvents.length==0)
            return new String[0];
        return featureSelectionEvents[featureSelectionEvents.length - 1].getTagset().getValues("feature").toArray(new String[0]);
    }

    /**
     *
     * By default, the predictors are getDefaultDBFeatureNames() except for getPredictionName().
     * That is, any inputs or derived features that do not start with underscore,
     * except for the last feature extractor which is assumed to be the prediction.
     * 
    **/
    public String[] getDefaultPredictorNames() throws Exception {
        Set<String> predictors = new HashSet<String>();
        predictors.addAll(Arrays.asList(getDefaultDBFeatureNames()));
        predictors.remove(getPredictionName());
        return predictors.toArray(new String[0]);
    }

    /**
     *
     * Select which features will be used by the model from this time forward.
     * This selection is stored in the RBB.
     * The features specified must be a subset of getDBFeatureNames, or an exception is raised.
     *
     */
    public void setPredictorNames(String[] features) throws Exception {
        if(features == null)
            throw new Exception("MLModel.setPredictorNames error: the list of features is null");
        if(features.length == 0)
            throw new Exception("MLModel.setPredictorNames error: the list of features is empty");

        Tagset tags = new Tagset();
        Set<String> possibleTags = new HashSet<String>(Arrays.asList(getDBFeatureNames()));
        for(String feature : features) {
            if(!possibleTags.contains(feature))
                throw new SQLException("Invalid feature " + feature + " specified");
            tags.add("feature", feature);
        }

        final Set<String> prevFeatures = new HashSet<String>(Arrays.asList(getPredictorNames()));

        recordUserInteraction("RBBML_Feature_Selection", tags, null, null, null);

        final Set<String> newFeatures = new HashSet<String>(Arrays.asList(getPredictorNames()));
    }

    /**
     *
     * Returns a minimal set of tags to uniquely identify
     * results from this model.  All RBB Events (or timeseries) created from this model have
     * at least these tags. Nothing BUT results from this model should have all these tags,
     * since they will be deleted every time the results are re-computed.
     *
     */
    public Tagset getMinimalResultTags() throws Exception {
        Tagset t = new Tagset();
        t.add("model", getName());
        t.add("type", "result");
        return t;
    }

    /**
     * <pre>
     * Get the tagset that will be attached to events or timeseries that are results
     * from this model.
     *
     * This includes:
     * 1) getMinimalResultTags() - model=<mymodel>,type=result
     * 2)  template tags of the form:
     * <inputName>.RBBID=<inputName>.RBBID
     * The value of these tags is replaced by the ID of the input for the problem instance, e.g.
     * leader.RBBID=1234
     *
     * Derived classes should override this to add more tags.
     *
     * Note: To match all results from this model, see getMinimalResultTags() instead.
     *
     * </pre>
     */
    public Tagset getResultTags() throws Exception {
        Tagset t = getMinimalResultTags();
        t.add("model", getName());
        t.add("type", "result");

        for(String inputName : getInputNames(PREDICTION))
            t.add(inputName+".RBBID", inputName+".RBBID");

        return t;
    }

    /**
     * Revert the model to the state of just after it was created - no training data or training events.
     */
    public void reset() throws Exception {
        rbb.db().createStatement().execute("call rbb_delete_events(('type','RBBML_Training_Event')); delete from RBBML_TRAINING_DATA;");
    }

    /**
     * Record an action the user took to modify the model.
     * The only required parameter is "type"
     */
    Event recordUserInteraction(String type, Tagset addTags, Double simTime, String sessionName, String userName) throws Exception
    {
        Tagset tags = new Tagset();
        tags.add("type", type);
        tags.add("superType", "userInteraction");
        if(simTime != null)
            tags.add("session_time", simTime.toString());
        if(sessionName != null)
            tags.add("session", sessionName);
        if(userName != null)
            tags.add("user", userName);
        if(addTags != null)
            tags.add(addTags);
        final Double time = System.currentTimeMillis()/1000.0;
        return new Event(rbb.db(), time, time, tags);
    }


    public static int codeSchemaVersion() {
        return 4;
    }


    /**
     * "delete" a training example, so it's no longer used in the model.
     *
     *  1) change type=RBBML_Training_Event to type=RBBML_Deleted_Training_Event in the original event.
     *  2) create a new user interaction event, type=RBBML_Training_Event_Deletion, whose parentID= specifies the ID of the deleted example.
     *  3) set RBBML_TRAINING_DATA.WEIGHT to
     *
     * @throws java.sql.SQLException
     */
    public void deleteTrainingExampleByID(long ID) throws Exception {

        // update the weight to a negative value so it won't be used by the model.
        String q = "UPDATE RBBML_TRAINING_DATA SET WEIGHT="+TRAINING_EXAMPLE_WEIGHT__DELETED+" WHERE TRAINING_EVENT="+ID+";";
        if(rbb.db().createStatement().executeUpdate(q) == 0)
            throw new SQLException("MLModel.deleteTrainingExampleByID: invalid ID " + ID + ": is not an RBBML_Training_Event");

        // update the type of the training event.
        H2SEvent.setTagsByID(rbb.db(), ID, "type=RBBML_Deleted_Training_Event");

        // record the deletion as a user interaction
        // recordUserInteraction("RBBML_Training_Event_Deletion", new Event[]{new H2Event(modelRBB,ID)}, null);
    }

    /**
     * Add the observations as training data.
     * The Observations must already include the predicted feature.
     *<p>
     * The MLCoordination argument is just there in case an override needs to retrieve any extra information.
     * The AEMASE Model uses it to attach a screenshot of the training example and attach it to the training event.
     */
    protected Event addTrainingData(RBB coordinationRBB, MLObservationSequence observationSequence, String label) throws Exception {
        if(observationSequence.size()==0)
            return null;

        Tagset inputTags = new Tagset();
        for(String inputName : getInputNames(TRAINING, false))
            inputTags.add(inputName+".RBBID", observationSequence.getMetadata().getFeatureEvent(inputName).getID().toString());

        inputTags.set("label", label);

        Event addExampleEvent = recordUserInteraction("RBBML_Training_Event",
                inputTags, observationSequence.getOldest().getTime(), null, null);

        // insert training data vectors.
        String[] dbFeatureNames = getDBFeatureNames();
        StringWriter sw = new StringWriter();
        sw.write("insert into RBBML_TRAINING_DATA values(?,?"); // make a slot for the TRAINING_EVENT and WEIGHT
        for(int i = 0; i < dbFeatureNames.length; ++i)
            sw.write(",?"); // make a slot for each feature.
        sw.write(");");

        java.sql.PreparedStatement prepStmt =
                rbb.db().prepareStatement(sw.toString());

        prepStmt.setLong(1, addExampleEvent.getID());
        prepStmt.setFloat(2, TRAINING_EXAMPLE_WEIGHT__DEFAULT); // default weight = 1.0f

        for (int iOb = 0; iOb < observationSequence.size(); ++iOb) {
            MLObservation ob = observationSequence.getOldest(iOb);
            // System.err.println("Adding training example:");
            for(int f = 0; f < dbFeatureNames.length; ++f) {
                prepStmt.setObject(f+3, ob.getFeature(dbFeatureNames[f])); // +3 because of TRAINING_EVENT and WEIGHT cols, and setobject is 1-based
               //  System.err.print("\t" + dbFeatureNames[f]);
//                for(int i = 0; i < ob.getFeatureAsFloats(dbFeatureNames[f]).length; ++i)
//                    System.err.print(" " + ob.getFeatureAsFloats(dbFeatureNames[f])[i]);
//                System.err.println("");
            }
            prepStmt.execute();
        }


        for(String feature : dbFeatureNames) {
            Timeseries ts = observationSequence.getFeatureTimeseries(feature);

            Tagset tags = new Tagset();
            tags.add("model", getName());
            tags.add("trainingExampleID", addExampleEvent.getID().toString());
            tags.add("type", "feature");
            tags.add("feature", feature);
            tags.add("sourceTagset", ts.getTagset().toString());
            // tags.add("sourceRBB", )
            // tags.add("sourceRBBID, )

            ts.setTagset(tags);
        }

        return addExampleEvent;
    }

    /**
     * count the training examples whose weight is > 0. 
     */
    int getNumTrainingExamples() throws Exception {
        ResultSet rs = rbb.db().createStatement().executeQuery("select count(*) from RBBML_TRAINING_DATA where WEIGHT > 0");
        rs.next();
        return rs.getInt(1);
    }

    /**
     * The timestep for training and evaluating this model.
     *<p>
     * The default is null, which avoids resampling, instead using the data Samples
     * as originally stored in the RBB.  This is generally good so long as
     * all the data are observed relatively frequently relative to the rate of change.
     *<p>
     * But if some timeseries are observed infrequently, and if they could be
     * the first input to a model, you probably want to override with a smaller
     * timestep to force resampling
     */
    public Double getTimestep() {
        return null;
    }

    /**
     * Retrieves the training data, according to the currently-selected
     * set of predictors and prediction.
     *
     * Only the training data with weight > 0 is returned.
     */
    public MLTrainingData.FromRBB getTrainingData() throws Exception {
        return new MLTrainingData.FromRBB(rbb, getPredictorNames(), getPredictionName());
    }

    public String toStringDetail() throws Exception {
        try {
            StringsWriter sw = new StringsWriter();

            sw.writeStrings("RBB ML Model: ", getName(), "\n");

            sw.write("Descriptor:\n");
            sw.writeQueryResults(rbb.db(), "select * from RBBML_DESCRIPTOR;");

            sw.writeStrings("training inputs: ");
            sw.writeJoin(", ", this.getInputNames(TRAINING, false));
            sw.writeStrings("\n");
            sw.writeStrings("training groups: ");
            sw.writeJoin(", ", this.getInputNames(TRAINING, true));
            sw.writeStrings("\n");
            sw.writeStrings("prediction inputs: ");
            sw.writeJoin(", ", this.getInputNames(PREDICTION, false));
            sw.writeStrings("\n");
            sw.writeStrings("prediction groups: ");
            sw.writeJoin(", ", this.getInputNames(PREDICTION, true));
            sw.writeStrings("\n");

            sw.writeStrings("Selected Features: ");
            sw.writeJoin(" ", this.getPredictorNames());
            sw.write("\n");

            sw.writeStrings("Available Features: ");
            sw.writeJoin(" ", this.getDBFeatureNames());
            sw.write("\n");

            sw.write("Training Data:\n");
            sw.writeQueryResults(rbb.db(), "select * from RBBML_TRAINING_DATA");

            ResultSet rs = H2SEvent.findTagCombinations(rbb.db(), "type", "superType=userInteraction");
            while(rs.next()) {
                Tagset tags = new Tagset((Object[])rs.getArray(1).getArray());
                sw.write(tags.getValue("type") + " User Interactions:\n");
                for(Event e : Event.find(rbb.db(), byTags(tags)))
                    sw.writeStrings("\t", e.toString(), "\n");
                sw.write("\n");
            }

            return sw.toString();
        } catch(SQLException e) {
            return "Error in MLModel.toStringDetail: "+e.toString();
        }
    }
}
