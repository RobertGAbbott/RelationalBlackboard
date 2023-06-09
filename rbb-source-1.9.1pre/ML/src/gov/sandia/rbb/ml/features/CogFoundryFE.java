/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;
import gov.sandia.cognition.evaluator.Evaluator;
import gov.sandia.cognition.evaluator.ValueMapper;
import gov.sandia.cognition.learning.algorithm.bayes.VectorNaiveBayesCategorizer;
import gov.sandia.cognition.learning.algorithm.nearest.KNearestNeighborExhaustive;
import gov.sandia.cognition.learning.algorithm.regression.LocallyWeightedFunction;
import gov.sandia.cognition.learning.algorithm.regression.LocallyWeightedFunction.Learner;
import gov.sandia.cognition.learning.algorithm.regression.MultivariateLinearRegression;
import gov.sandia.cognition.learning.algorithm.svm.SuccessiveOverrelaxation;
import gov.sandia.cognition.learning.algorithm.tree.CategorizationTreeLearner;
import gov.sandia.cognition.learning.algorithm.tree.VectorThresholdHellingerDistanceLearner;
import gov.sandia.cognition.learning.algorithm.tree.VectorThresholdInformationGainLearner;


import gov.sandia.cognition.math.matrix.Vector;
import gov.sandia.cognition.math.matrix.mtj.DenseVector;
import gov.sandia.cognition.learning.data.DefaultInputOutputPair;
import gov.sandia.cognition.learning.data.feature.MultivariateDecorrelator;
import gov.sandia.cognition.learning.function.distance.EuclideanDistanceMetric;
import gov.sandia.cognition.learning.function.kernel.RadialBasisKernel;
import gov.sandia.cognition.math.RingAverager;
import gov.sandia.cognition.math.matrix.Vectorizable;
import gov.sandia.cognition.statistics.distribution.UnivariateGaussian;
import gov.sandia.rbb.ml.MLTrainingData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * CogFoundryFE implements an RBB ML Feature Extractor using a Cognitive Foundry Evaluator.
 * This is where the RBB and Cognitive Foundry come together.
 *
 * This class is also the home of utility methods to convert between datatypes for Foundry vs RBB,
 * e.g. Vector <-> Float[]
 *
 * @author rgabbot
 */
public class CogFoundryFE extends MLFeatureExtractor
{
    /**
     * This a Cognitive Foundry Feature Extractor.
     *<p>
     * All the constructors take a ResultSet which has been loaded up with
     * the training examples.  This is normally created by MLModel.getTrainingData.
     * The last column is the response; each other
     * column is a predictor.  All the columns contain Array datatypes, since
     * the predictors and response may be multi-dimensional.
     */
    static final gov.sandia.cognition.math.matrix.VectorFactory<DenseVector> vectorFactory = gov.sandia.cognition.math.matrix.mtj.DenseVectorFactoryMTJ.INSTANCE;
    String[] inputFeatureNames;
    String outputName;

    /**
     * Here is here is the cognitive foundry model.
     */
    Evaluator<Vectorizable,Vector> model;

//        class Model {
//        ArrayList<DefaultInputOutputPair<Vector,Vector>> trainingData;
//        Evaluator<Vectorizable,Vector> model;
//    }
//
//    Tagset modelKey;
//    Map<Tagset, Model> models;
//
//    void setTrainingData(MLTrainingData td) {
//    }


    /*
     * Several of the algorithms can be constructed from the same parameters.
     * This function supports creating any of them based on an enum.
     *<p>
     * Default is a stab at a reasonable machine learning model for most tasks given minimal parameters.
     * the problem with anything that uses probability distributions is
     * they all seem to get very unstable when the probabilities are very low,
     * i.e. there are no good training examples to make a prediction.
     * The problem with LWLR as a default is it will extrapolate wildly
     * The problem with SVM as a default is that it's a binary predictor
     */
    public static enum Algorithm { Default, KNN, DecisionTree, LWLR, NaiveBayes, SVM, LookupTable };

    public static MLFeatureExtractor createAlgorithm(String outputName, String[] inputNames, Algorithm algorithm, MLTrainingData trainingData, MLFeatureExtractor nextFeatureExtractor) throws Exception {
        if(algorithm==Algorithm.Default)
            algorithm = Algorithm.KNN;
        switch(algorithm) {
//            case K1NN: return create1NN(outputName, inputNames, trainingData, nextFeatureExtractor);
            case KNN: return createNN(outputName, inputNames, trainingData, nextFeatureExtractor);
            case DecisionTree: return createDecisionTree(outputName, inputNames, trainingData, nextFeatureExtractor);
            case LWLR: return createLWLR(outputName, inputNames, trainingData, nextFeatureExtractor);
            case NaiveBayes: return createNaiveBayes(outputName, inputNames, trainingData, nextFeatureExtractor);
            case SVM: return createSVM(outputName, inputNames, trainingData, nextFeatureExtractor);
            case LookupTable: return createLookupTable(outputName, inputNames, trainingData, nextFeatureExtractor);
            default:
                throw new IllegalArgumentException("Invalid algorithm: "+algorithm.toString()+" specified");
        }
    }

    /**
     * This is a stab at a reasonable machine learning model for most tasks given minimal parameters.
     *
     */
    public static MLFeatureExtractor createDefault(String outputName, String[] inputNames, MLTrainingData td, MLFeatureExtractor nextFeatureExtractor) throws Exception {
        //// the problem with anything that uses probability distributions is
        // they all seem to get very unstable when the probabilities are very low,
        // i.e. there are no good training examples to make a prediction.
        // return CogFoundryFE.createNaiveBayes(getPredictionName(), this.getPredictorNames(), db(), null);

        //// the problem with LWLR as a default is it will extrapolate wildly
        // return CogFoundryFE.createLWLR(getPredictionName(), this.getPredictorNames(), db(), null);

        //// the problem with SVM as a default is that it's a binary predictor
        // return CogFoundryFE.createSVM(getPredictionName(), this.getPredictorNames(), db(), null);

        return createNN(outputName, inputNames, td, nextFeatureExtractor);
    }

    /**
     * Create nearest neighbor evaluator with a reasonable guess at K.
     * K is simply 5, reduced to a smaller K if there are only a few training examples.
     */
    public static MLFeatureExtractor createNN(String outputName, String[] inputNames, MLTrainingData td, MLFeatureExtractor nextFeatureExtractor) throws Exception {

        int k = 5;

        CogFoundryFE fe = new CogFoundryFE(outputName, inputNames, nextFeatureExtractor);

        // NN won't work well unless the data is decorrelated
        DataNormalizer norm = new DataNormalizer();
        ArrayList<DefaultInputOutputPair<Vector,Vector>> normalizedTrainingData = norm.init(td);

        k = Math.min(k, normalizedTrainingData.size()/3); // if we only have, say, two training examples, better to use just the nearest one than both.
        k = Math.max(k, 1); // but k must be at least 1.

        norm.nextEvaluator = new KNearestNeighborExhaustive<Vectorizable,Vector>(
                k,
                normalizedTrainingData,
                new EuclideanDistanceMetric(),
                new RingAverager<Vector>());
//                new MostFrequentSummarizer<Vector>());

        fe.model = norm;

        return fe;
    }

    /**
     * The Lookup Table does not generalize; it simply looks to see if there is a training
     * example that matches the query, and if so returns its value; otherwise no output is set
     * (it will be null in the observation).
     */
    public static MLFeatureExtractor createLookupTable(String outputName, String[] inputNames, MLTrainingData td, MLFeatureExtractor nextFeatureExtractor) throws Exception {
        Map<Vectorizable, Vector> map = new HashMap<Vectorizable, Vector>();
        for(DefaultInputOutputPair<Vector,Vector> pair : getTrainingData(td))
            map.put(pair.getInput(), pair.getOutput());

        System.err.println("LookupTable contents:");
        for(Vectorizable v : map.keySet())
            System.err.println(v+" -> "+map.get(v));

        CogFoundryFE fe = new CogFoundryFE(outputName, inputNames, nextFeatureExtractor);
        fe.model = ValueMapper.create(map);

//        CogFoundryFE fe = new CogFoundryFE(outputName, inputNames, nextFeatureExtractor);
//
//        fe.model = new KNearestNeighborExhaustive<Vectorizable,Vector>(
//                1,
//                getTrainingData(td),
//                new EuclideanDistanceMetric(),
//                new RingAverager<Vector>()) {
//
//        };
//                new MostFrequentSummarizer<Vector>());

        return fe;
    }

//    public static CogFoundryFE createWeightedKNN(String outputName, String[] inputFeatureNames, int k, Connection modelDB, MLFeatureExtractor nextFeatureExtractor) throws Exception {
//
//        CogFoundryFE fe = new CogFoundryFE(outputName, inputFeatureNames, nextFeatureExtractor);
//        // MulitivariateLinearRegression
//
//        final Learner<Vector, Vector> learner = new LocallyWeightedFunction.Learner<Vector, Vector>(new RadialBasisKernel(), new KNearestNeighborExhaustive.Learner<Vector,Vector>(k, new EuclideanDistanceMetric(), new RingAverager<Vector>()));
//        final LocallyWeightedFunction<Vector, Vector> model = learner.learn(getTrainingData(inputFeatureNames, outputName, modelDB));
//
//        // this is a bit of glue to resolve an inheritance + java generics nightmare in the foundry.
//        // I think the root of the problem is MultivariateLinearRegression used Vector in one of its "implmements" declarations
//        // where it should have used Vectorizable.
//        fe.model = new Evaluator<Vectorizable,Vector>() {
//            @Override
//            public Vector evaluate(Vectorizable input) {
//                return model.evaluate(input.convertToVector());
//            }
//        };
//        return fe;
//    }
//

    public static CogFoundryFE createNaiveBayes(String outputName, String[] inputFeatureNames, MLTrainingData td, MLFeatureExtractor nextFeatureExtractor)
            throws Exception
    {
        CogFoundryFE fe = new CogFoundryFE(outputName, inputFeatureNames, nextFeatureExtractor);

        VectorNaiveBayesCategorizer.Learner<Vector, UnivariateGaussian.PDF> learner =
            new VectorNaiveBayesCategorizer.Learner<Vector, UnivariateGaussian.PDF>(
                new UnivariateGaussian.MaximumLikelihoodEstimator());

        fe.model = learner.learn(getTrainingData(td));

        return fe;
    }

    public static CogFoundryFE createDecisionTree(String outputName, String[] inputFeatureNames, MLTrainingData td, MLFeatureExtractor nextFeatureExtractor)
            throws Exception
    {
        CogFoundryFE fe = new CogFoundryFE(outputName, inputFeatureNames, nextFeatureExtractor);

        VectorThresholdInformationGainLearner<Vector> deciderLearner =
            new VectorThresholdInformationGainLearner<Vector>();
        CategorizationTreeLearner<Vectorizable, Vector> instance =
            new CategorizationTreeLearner<Vectorizable, Vector>(deciderLearner);
        instance.setDeciderLearner(new VectorThresholdHellingerDistanceLearner<Vector>());

        fe.model = instance.learn(getTrainingData(td));

        return fe;
    }

    public static CogFoundryFE createSVM(String outputName, String[] inputFeatureNames, MLTrainingData td, MLFeatureExtractor nextFeatureExtractor) throws Exception
    {
        CogFoundryFE fe = new CogFoundryFE(outputName, inputFeatureNames, nextFeatureExtractor);

//        final SuccessiveOverrelaxation<Vector> instance = new SuccessiveOverrelaxation<Vector>(LinearKernel.getInstance());
        final SuccessiveOverrelaxation<Vector> instance = new SuccessiveOverrelaxation<Vector>(new RadialBasisKernel());
//        final SuccessiveOverrelaxation<Vector> instance = new SuccessiveOverrelaxation<Vector>(new PolynomialKernel());
        instance.setMaxIterations(1000);
        DataNormalizer dataNormalizer = new DataNormalizer();
        dataNormalizer.nextEvaluator = new BooleanToVectorConverter(instance.learn(convertNormalizedToBinary(dataNormalizer.init(td))));
        fe.model = dataNormalizer;

        return fe;
    }

    public static CogFoundryFE createLWLR(String outputName, String[] inputFeatureNames, MLTrainingData td, MLFeatureExtractor nextFeatureExtractor) throws Exception {

        CogFoundryFE fe = new CogFoundryFE(outputName, inputFeatureNames, nextFeatureExtractor);
        // MulitivariateLinearRegression
        
        final Learner<Vector, Vector> learner = new LocallyWeightedFunction.Learner<Vector, Vector>(new RadialBasisKernel(), new MultivariateLinearRegression());
        final LocallyWeightedFunction<Vector, Vector> model = learner.learn(getTrainingData(td));

        // this is a bit of glue to resolve an inheritance + java generics nightmare in the foundry.
        // I think the root of the problem is MultivariateLinearRegression used Vector in one of its "implmements" declarations
        // where it should have used Vectorizable.
        fe.model = new Evaluator<Vectorizable,Vector>() {
            @Override
            public Vector evaluate(Vectorizable input) {
                return model.evaluate(input.convertToVector());
            }
        };
        return fe;
    }

//    public static CogFoundryFE createDirichletProcessClustering(String outputName, String[] inputFeatureNames, Connection modelDB, MLFeatureExtractor nextFeatureExtractor)
//            throws Exception
//    {
//        CogFoundryFE fe = new CogFoundryFE(outputName, inputFeatureNames, nextFeatureExtractor);
//
//        DirichletProcessClustering instance = new DirichletProcessClustering();
//
//        ArrayList<GaussianCluster> clusters = instance.learn(getTrainingData(inputFeatureNames, modelDB));
//
//        return fe;
//    }


    /**
     * Wraps an evaluator with a boolean output type to a Vector output type which is what CogFoundryFE outputs.
     */
    static class BooleanToVectorConverter implements Evaluator<Vectorizable,Vector> {
        Evaluator<Vector,Boolean> binaryClassifier;

        BooleanToVectorConverter(Evaluator<Vector,Boolean> classifier) {
            binaryClassifier = classifier;
        }

        @Override
        public Vector evaluate(Vectorizable input) {
            final Boolean result = binaryClassifier.evaluate(input.convertToVector());
            if(result == true)
                return CogFoundryFE.vectorFactory.createVector1D(1.0f);
            else
                return CogFoundryFE.vectorFactory.createVector1D(-1.0f);
        }
    }


    /**
     * Wraps an evaluator with a decorrelator (normalizes variance in all directions)
     */
    static class DataNormalizer implements Evaluator<Vectorizable,Vector> {
        Evaluator<Vectorizable,Vector> nextEvaluator;
        Evaluator<Vectorizable,Vector> dataNormalizer;

        /**
         * Initialize the dataNormalizer on the specified training data and
         * return a copy of the training data with normalized inputs.
         */
        public ArrayList<DefaultInputOutputPair<Vector,Vector>> init(MLTrainingData td) throws Exception {
            ArrayList<DefaultInputOutputPair<Vector,Vector>> xy = getTrainingData(td);
            ArrayList<Vector> x = new ArrayList<Vector>();
            for(DefaultInputOutputPair<Vector,Vector> xy0 : xy)
                x.add(xy0.getInput());
            dataNormalizer = MultivariateDecorrelator.learnDiagonalCovariance(x, 1e-5);
            for(DefaultInputOutputPair<Vector,Vector> xy0 : xy)
                xy0.setInput(dataNormalizer.evaluate(xy0.getInput()));
            // printdata("normalized:", xy);
            return xy;
        }
        
      
        @Override
        public Vector evaluate(Vectorizable input) {
            return nextEvaluator.evaluate(dataNormalizer.evaluate(input));
        }
    }

//    public static CogFoundryFE createMixtureOfGaussians(String outputName, String[] inputFeatureNames, Connection modelDB, MLFeatureExtractor nextFeatureExtractor)
//            throws Exception
//    {
//        CogFoundryFE fe = new CogFoundryFE(outputName, inputFeatureNames, nextFeatureExtractor);
//
//        VectorNaiveBayesCategorizer.Learner<Float, MixtureOfGaussians.PDF> learner;
////                ;=
////            new VectorNaiveBayesCategorizer.Learner<Float, UnivariateGaussian.PDF>(
////                new UnivariateGaussian.MaximumLikelihoodEstimator());
//
//        fe.model = learner.learn(getTrainingData(inputFeatureNames, modelDB));
//
//        return fe;
//    }

    /**
     * Note, this constructor does not create an instance that is ready to use -
     * you must still set this.model to some cognitive cognitive foundry evaluator (i.e., a regression algorithm)
     *
     * Normally this is done by using the static constructor utility functions above instead, or making something similar.
     */
    public CogFoundryFE(String outputName, String[] inputFeatureNames, MLFeatureExtractor nextFeatureExtractor) throws Exception
    {
        super(nextFeatureExtractor, outputName);
        this.inputFeatureNames = inputFeatureNames;
    }

//    private static class DenseVectorH extends DenseVector implements Hashable {
//        DenseVectorH(int dim) { super(dim); }
//
//    }

    public static ArrayList<DefaultInputOutputPair<Vector,Vector>> getTrainingData(MLTrainingData td) throws Exception {
        // retrieve the data.
        ArrayList<DefaultInputOutputPair<Vector,Vector>> data = new ArrayList<DefaultInputOutputPair<Vector,Vector>>();
        int totalDim=0;
        while(td.next())
        {
            if(totalDim == 0) { // i.e. the first time.  Each row must have the same total dim.
               for(int i = 0; i < td.getNumPredictors(); ++i)
                    totalDim += td.getPredictor(i).length;
            }
            // get the predictors.
            Vector x = vectorFactory.createVector(totalDim);
//            Vector x = new DenseVectorH(totalDim);

            int dim = 0;
            for(int i = 0; i < td.getNumPredictors(); ++i) {  // -1 beause the final column is the response variable.
                Object[] a = td.getPredictor(i); // +1 because getArray is a 1-based index.
                arrayToVector(a, x, dim);
                dim += a.length;
            }
            // get the response.
            Vector y = arrayToVector(td.getResponse());

            data.add(new DefaultInputOutputPair<Vector,Vector>(x, y));
        }
       return data;
    }

    private static void printdata(String msg, ArrayList<DefaultInputOutputPair<Vector,Vector>> data) {
        System.err.println(msg + " - " + data.size() + " elements");
        for(DefaultInputOutputPair<Vector,Vector> xy : data)
            System.err.println("\t"+xy.getInput().toString()+"\t"+xy.getOutput().toString());
    }

    public static ArrayList<DefaultInputOutputPair<Vector,Boolean>> convertNormalizedToBinary(ArrayList<DefaultInputOutputPair<Vector,Vector>> data) throws Exception {
        ArrayList<DefaultInputOutputPair<Vector,Boolean>> binaryData = new ArrayList<DefaultInputOutputPair<Vector,Boolean>>();
        for(DefaultInputOutputPair<Vector,Vector> floatData : data)
            binaryData.add(new DefaultInputOutputPair<Vector,Boolean>(floatData.getInput(), floatData.getOutput().getElement(0) > 0.0f ? true : false));
        return binaryData;
    }

    public static Float[] concatFeatures(String[] featureNames, MLObservation obs) {
        // determine the sum of dimensions of input featureNames.
        int dim = 0;
        for(int i = 0; i < featureNames.length; ++i)
            dim += obs.getFeatureAsFloats(featureNames[i]).length;

        Float[] output = new Float[dim];
        int outputDim=0;
        for(int i = 0; i < featureNames.length; ++i) {
            Float[] x0 = obs.getFeatureAsFloats(featureNames[i]); // get feature fector for this feature
            // now append x0 to output.
            for(int j = 0; j < x0.length; ++j, ++outputDim)
                output[outputDim] = x0[j];
       }
       return output;
    }
    
    @Override
    public void observe(MLObservation obs) throws Exception {

        if(model==null)
            throw new Exception("Error in CogFoundryFE.observe: the subclass "+this.getClass().getName()+" did not set CogFoundryFE.model during construction!");

        Float[] x = concatFeatures(this.inputFeatureNames, obs);

        // copy over to a feature vector for the cognitive foundry.
        Vector vx =  arrayToVector(x);

        // System.err.println(obs.getTime().toString()+" Evaluating "+vx);

        Vector vy = this.model.evaluate(vx);

        if(vy != null)
            obs.setFeature(this.getOutputName(0), vectorToFloats(vy));

        // System.err.println("The predicted value for " + vx.toString() + " is " + vy.toString());

        super.observe(obs);
    }

    /**
     * Copy the entire contents of a into new vector v with length a.length
     */
    public static Vector arrayToVector(Object[] a) {
        Vector result = vectorFactory.createVector(a.length);
        arrayToVector(a, result, 0);
        return result;
    }

    /**
     * Copy the entire contents of a into vector v.
     * The initial output position within v is iv.
     */
    public static void arrayToVector(Object[] a, Vector v, int iv) {
        for(int j = 0; j < a.length; ++j) {
            Float x;
            if(a[j] instanceof Float)
                x = (Float) a[j];
            else
                x = Float.parseFloat(a[j].toString());
            v.setElement(iv++, x);
        }
    }

    /**
     * Concatenate multiple arrays into a single vector.
     */
    public static Vector arraysToVector(ArrayList<Object[]> a) {
        int totalDim = 0;
        for(Object[] a0 : a)
            totalDim += a0.length;
        Vector v = vectorFactory.createVector(totalDim);
        int pos = 0;
        for(Object[] a0 : a) {
            arrayToVector(a0, v, pos);
            pos += a0.length;
        }
        return v;
    }

    public static Float[] vectorToFloats(Vector v) {
        Float[] x = new Float[v.getDimensionality()];
        for(int i = 0; i < v.getDimensionality(); ++i)
            x[i] = (float) v.getElement(i);
        return x;
    }
}
