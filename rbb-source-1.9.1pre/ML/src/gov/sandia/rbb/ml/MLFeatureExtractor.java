/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml;

import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.ml.MLObservation.Metadata;
import gov.sandia.rbb.util.StringsWriter;
import java.lang.reflect.Constructor;
import java.util.ArrayList;

/**
 * A Feature Extractor looks at each MLObservation in a problem instance and
 * does "something" - typically creating a derived feature, such as 
 * computing the distance between two other features that represent positions.
 *
 * This base class is a no-op feature extractor.
 * DistanceFE is an example of a simple, typical feature extractor.
 *
 * @author rgabbot
 */
 public class MLFeatureExtractor implements Cloneable {

    /**
    * Feature Extractors are linked in a chain, so it is the job of each one to
    * control the execution of its successors.  The reason they're linked like this
    * (instead of e.g. storing them all in an external list) is because each extractor
    * controls the execution of its successors - it might delay calling them if it needs
    * to see subsequent Observations before adding its feature, or it might not call them
    * at all depending on the content of the Observations it sees.
    */
    protected MLFeatureExtractor nextFeatureExtractor;

    /**
     * The outputs argument specifies the names of the features this object will extract.
     * These names are used as column names in the database so they must be valid SQL identifiers.
     * Feature names in a feature extraction chain must be unique.
     * The return value may be empty but must not be null.
     */
    private String[] outputs;

    public MLFeatureExtractor(MLFeatureExtractor nextFeatureExtractor, String... outputs) {
        this.nextFeatureExtractor = nextFeatureExtractor;
        this.outputs = outputs;
    }

    /**
     * Get the names of the features to be created by this MLFeatureExtractor.
     * There should be no need to override this, but if so, the return value shouldn't change after construction.
     *
     */
    public String getOutputName(int i) { return outputs[i]; };
    public int getNumOutputs() { return outputs.length; };

    /**
    * This is called once for each problem instance, before the first observation.
    * 
    * Stateful feature extractors must reset their state.
    * non-stateful feature extractors probably don't need to override this.
     *
    */
    public void init(double time, MLObservation.Metadata md) throws Exception {
        if(this.nextFeatureExtractor != null)
            this.nextFeatureExtractor.init(time, md);
    }

    /**
     * This is called for each timestep of a problem evaluation.
     *  The override should typically:
     *
     * 1) Get the values of input features from the MLObservation, e.g. obs.getFeatureAsFloats("inputName")
     * 2) Compute the output feature and store this result in the MLObservation, e.g. obs.setFeatureAsFloats("outputName")
     * 3) Continue the chain by calling this method - super.observe()
     *
     * Certain (special) Feature Extractors have side effects or need additional information
     * from the RBB that provided the input features, or the RBB where results will be stored;
     * in this case see setRBB()
     *
     * The feature extractor must not modify the values of previously extracted features,
     * such as by modifying the contents of an array set as a feature of a previous observation.
     * Other parts of the software rely on being able to store Observations without them being retroactively changed.
     *
     */
    public void observe(MLObservation obs) throws Exception {
        if(this.nextFeatureExtractor != null)
            this.nextFeatureExtractor.observe(obs);
    }

    /**
     * This is called after the last MLObservation in a sequence to do any necessary cleanup.
     * 
     */
    public void done(double time) throws Exception {
        if(this.nextFeatureExtractor != null)
            this.nextFeatureExtractor.done(time);
    }

    /**
     * This doesn't need to be overridden unless a feature extractor uses
     * values from the past (or future, for cooldown)
     *
     * Some feature extractors cannot produce the feature until after making some observations.
     * An example is a smoothing filter; the value for time t may be the result of
     * values both before and after t.  So to get the value for time t by random
     * access, it is not sufficient to retrieve just the value at time t.
     *
     * Such feature extractors should override warmup() if they need observations
     * prior to producing an observation, and likewise cooldown() to get observations
     * subsequent to an estimate for a time.  In both cases 0 or a positive value
     * should be returned (even though warmup() is effectively reaching into the past).
     *
     * A feature extractor can also return a warmup/cooldown value of 'null', meaning
     * it cannot place a bound on how much history it needs.  In this case random
     * access is not possible and the problem will be evaluated from the beginning
     * (or to the end, respectively)
     *
     * If one previous observation is needed, but it is unknown how far in advance
     * it might arrive, the override can return a very small value such as 1e-6
     * The reason this works is that the software tries to round the warmup up
     * to the previous observation so the requested warmup period is met or exceeded
     * rather than not met.
     *
     * No time conversion is performed on this time so it should be reported in the
     * 'native' unit of time for the application.
     *
     */
    public Double getWarmup() {
        return 0.0;
    }

    /**
     * See warmup()
     */
    public Double getCooldown() {
        return 0.0;
    }

    /**
     * max warmup of this or any successor.  Shouldn't need to be overridden.
     */
    Double getMaxWarmup() {
        if(nextFeatureExtractor == null)
            return getWarmup();
        Double w = nextFeatureExtractor.getMaxWarmup();
        if(w==null || getWarmup()==null)
            return null;
        return Math.max(getWarmup(), w);
    }

    /**
     * max cooldown of this or any successor.  Shouldn't need to be overridden.
     */
    Double getMaxCooldown() {
        if(nextFeatureExtractor == null)
            return getCooldown();
        Double w = nextFeatureExtractor.getMaxCooldown();
        if(w==null || getCooldown()==null)
            return null;
        return Math.max(getCooldown(), w);
    }

    /**
     *
     * Certain special feature extractors have side effects or require
     * extra information from the RBBs for a particular application.
     * They can call rbbml.getRBB() to obtain the required RBB.
     *<p>
     * In most cases this is not necessary and should be avoided -
     * typical feature extractors should use information from the
     * Observation sequence and store a new feature back to it.
     *<p>
     * Also note that the feature extractor's constructor will not
     * have access to the RBBML instance, since setRBBML won't have been set yet.
     *
     */
    public void setRBBML(RBBML rbbml) {
        this.rbbml = rbbml;
        if(this.nextFeatureExtractor != null)
            this.nextFeatureExtractor.setRBBML(rbbml);
    }
    protected RBBML rbbml;


    /**
     * Append a feature extractor to the tail of of the chain.
     * Returns the head of the chain (this).
     */
    public MLFeatureExtractor addToChain(MLFeatureExtractor tail) {
       if(this.nextFeatureExtractor == null)
           this.nextFeatureExtractor = tail;
       else
           this.nextFeatureExtractor.addToChain(tail);
       return this;
    }

    public MLFeatureExtractor getTail() {
       if(nextFeatureExtractor == null)
           return this;
       else
           return nextFeatureExtractor.getTail();
    }

    public void batchDone() {
        if(nextFeatureExtractor != null)
            nextFeatureExtractor.batchDone();
    }

    /**
     * <pre>
     * There is a special rule for calling MLFeatureExtractor.clone to make things easier.
     * A Feature Extractor cannot be cloned between init() and done().
     * So it is only necessary to override clone in a subclass if it has
     * mutable members that are not re-allocated by init().
     *
     * For incremental problem evaluation, multiple
     * problems are evaluated concurrently and each needs its own copy of the feature
     * extraction chain, created by clone()
     *
     * Even for batch processing, where problems are evaluated sequentially,
     * clone is necessary because RBB ML will use one copy of the feature extraction
     * chain for creating training examples, and a separate one for evaluating the scenario.
     *</pre>
     */
    @Override 
    public MLFeatureExtractor clone() throws CloneNotSupportedException {
        MLFeatureExtractor result = (MLFeatureExtractor) super.clone();
        if(this.nextFeatureExtractor != null)
            result.nextFeatureExtractor = this.nextFeatureExtractor.clone();
        return result;
    }

    /**
     * Add the output features from this and all subsequent features in this chain to the feature set.
     * This normally doesn't need to be overridden.
     */
    public void addSelfToFeatureNames(Metadata fs) {
        for(int i = 0; i < getNumOutputs(); ++i)
            fs.add(getOutputName(i), null);
        if(nextFeatureExtractor != null)
            nextFeatureExtractor.addSelfToFeatureNames(fs);
    }

    /**
     * Instantiate a subclass MLFeatureExtractor given its class name, and
     * an array of arguments, which are strings that will be converted to the
     * types needed by the constructor.
     *
     * If a subclass has multiple constructors, the first one with the right number
     * of arguments and parameter types convertible from the provided args is called.
     *
     * As a special case, MLFeatureExtractor parameters must not be specified -
     * they are passed to the constructor as null.
     *
     */
    public static MLFeatureExtractor fromStrings(String className, String[] args) throws Exception {
        Class c = null;

        ArrayList<String> classNameVariants = new ArrayList<String>();
        classNameVariants.add(className);
        classNameVariants.add("gov.sandia.rbb.ml.features."+className);

        // allow specifying a nested class with a . instead of a $
        // e.g. TimeseriesGateFE.EntersArea instead of TimeseriesGateFE$EntersArea
        String[] parts = className.split("\\.");
        if(parts.length > 1) {
            String nestedClassName = parts[0];
            for(int i = 1; i < parts.length-1; ++i)
                nestedClassName += "."+parts[i];
            nestedClassName += "$" +parts[parts.length-1];
            
            classNameVariants.add(nestedClassName);
            classNameVariants.add("gov.sandia.rbb.ml.features."+nestedClassName);
        }

        for(String className0 : classNameVariants) // try all the variants and quit on the first one.
        try {
            c = Class.forName(className0);
            className = className0; ////// if one works, update className parameter with that.
            break;
        } catch(ClassNotFoundException e) {
        }
            
        if(c==null)
            c = Class.forName(className); // throw the exception for the name the caller specified.

        Constructor[] constructors = c.getConstructors();
        CONSTRUCTORS: for(Constructor constructor : constructors) try {
            if(!constructor.getDeclaringClass().getName().equals(className))
                continue; // only call constructors from the most derived class.
            Class[] paramTypes = constructor.getParameterTypes();
            ArrayList<Object> params = new ArrayList<Object>();
            int iArg=0;
            for(Class paramType : paramTypes) {

                // most constructors take a "next" feature extractor, which can be null during construction,
                // and cannot be specified when constructing from args.  All FeatureExtractor args must be null.
                if(paramType.equals(MLFeatureExtractor.class) || 
                        (paramType.getSuperclass()!=null && paramType.getSuperclass().equals(MLFeatureExtractor.class))) {
                    params.add(null);
                    continue; // this does not consume an arg - no arg may be specified for FeatureExtractor parameters.
                }

                // all other parameter types consume exactly 1 arg.

                // don't overrun the args array (not enough args were passed for this constructor)
                if(iArg >= args.length)
                    continue CONSTRUCTORS;

                if(paramType.equals(String.class))
                    params.add(args[iArg].equalsIgnoreCase("null") ? null : args[iArg]);
                else if(paramType.equals(Boolean.class) || paramType.equals(boolean.class))
                    params.add(args[iArg].equalsIgnoreCase("null") ? null : Boolean.parseBoolean(args[iArg]));
                else if(paramType.equals(Integer.class) || paramType.equals(int.class))
                    params.add(args[iArg].equalsIgnoreCase("null") ? null : Integer.parseInt(args[iArg]));
                else if(paramType.equals(Float.class) || paramType.equals(float.class))
                    params.add(args[iArg].equalsIgnoreCase("null") ? null : Float.parseFloat(args[iArg]));
                else if(paramType.equals(Double.class) || paramType.equals(double.class))
                    params.add(args[iArg].equalsIgnoreCase("null") ? null : Double.parseDouble(args[iArg]));
                else if(paramType.getSuperclass()!=null && paramType.getSuperclass().equals(Enum.class))
                    params.add(args[iArg].equalsIgnoreCase("null") ? null : Enum.valueOf(paramType, args[iArg]));
                else if(paramType.equals(Tagset.class))
                    params.add(args[iArg].equalsIgnoreCase("null") ? null : new Tagset(args[iArg]));
                else if(paramType.equals(String[].class))
                    params.add(args[iArg].equalsIgnoreCase("null") ? null : args[iArg].split(","));
                else if(paramType.equals(Float[].class)) {
                    if(args[iArg].equalsIgnoreCase("null"))
                        params.add(null);
                    else {
                        String[] a = args[iArg].split(",");
                        Float[] x = new Float[a.length];
                        for(int i = 0; i < a.length; ++i)
                            x[i] = Float.parseFloat(a[i]);
                        params.add(x);
                    }
                }
                else {
                    System.err.println("Warning: a constructor for "+className+" has a parameter of type "+paramType.getName()+", which is not (yet) supported by MLFeatureExtractor.fromStrings");
                    continue CONSTRUCTORS;
                }

                ++iArg;
            }

            if(iArg < args.length) // this constructor didn't consume all the args so don't try to call it.
                continue CONSTRUCTORS;

            // the right number of args was specified and they were all converted OK, so try this one.

            // System.err.println("Constructing "+c.getName()+" with "+params.size()+" args: "+StringsWriter.join(",", args));

            return (MLFeatureExtractor) constructor.newInstance(params.toArray());
        } catch(NumberFormatException e) {
            // if there is a number format exception parsing args for a constructor,
            // assume it wasn't the right constructor and move on.
            // May need to add other types of exceptions, or continue on exceptions generally...
        }


        throw new Exception("Invalid args for constructing "+className+".  No constructors take "+args.length+" args: "+StringsWriter.join(":",args)+".  Note: The delimeter in MLMain for args is ':', not whitespace.");
     }
}
