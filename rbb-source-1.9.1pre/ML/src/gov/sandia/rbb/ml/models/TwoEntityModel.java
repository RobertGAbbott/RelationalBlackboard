/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.models;

import gov.sandia.rbb.ml.MLModel;
import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.RBBML.Mode;
import gov.sandia.rbb.ml.features.*;
import gov.sandia.rbb.util.StringsWriter;

/**
 * Implementation of MLModel for flagging spatial relations between pairs of positions.
 *
 */
public class TwoEntityModel extends MLModel {

    public TwoEntityModel() { };

//    private float supervisedScore = 0.0f;

    private CogFoundryFE.Algorithm algorithm = CogFoundryFE.Algorithm.Default;

//    @Override
//    public void parseTrainingArgs(String... args) throws Exception {
//        if(args.length != 1)
//            throw new Exception("TwoEntityModel.parseTrainingArgs takes 1 arg - the supervised score.  Got: "+StringsWriter.join(" ",args));
//        supervisedScore = Float.parseFloat(args[0]);
//    }
//
//    @Override
//    public void parsePredictionArgs(String... args) throws Exception {
//        int iArg=0;
//        for(; iArg < args.length; ++iArg) {
//            if(args[iArg].equalsIgnoreCase("-algorithm")) {
//                String usage = "-algorithm requires an argument which is one of: ";
//                for(CogFoundryFE.Algorithm algo : CogFoundryFE.Algorithm.class.getEnumConstants())
//                    usage += " " + algo.toString();
//                if(++iArg==args.length)
//                    throw new Exception(usage);
//                try {
//                    this.algorithm = CogFoundryFE.Algorithm.valueOf(args[iArg]);
//                } catch(Throwable t) {
//                    throw new Exception(usage);
//                }
//            }
//            else {
//                throw new Exception(this.getClass().getName()+": unrecognized model-specific parameter "+args[iArg]);
//            }
//        }
//    }

    // returns the part of the feature extraction chain common
    // to training and prediction.
    protected MLFeatureExtractor commonFeatures() throws Exception {
        return
            new DistanceFE("Range", "Position_A", "Position_B", DistanceFE.DistanceType.SCALAR,

            new RateFE("_Closure_Rate", "Range",
            new SmoothingFE("Closure_Rate", "_Closure_Rate", 0.5, 0.5,

            new RateFE("_Heading_A", "Position_A",
            new SmoothingFE("Heading_A", "_Heading_A", 0.5, 0.5,

            new RateFE("_Heading_B", "Position_B",
            new SmoothingFE("Heading_B", "_Heading_B", 0.5, 0.5,

            new AngleBetweenVectorsFE("Angle_Off", "Heading_A", "Heading_B",

            // if Aspect_A is 0, B is behind A.  If 180, B is in front of A
            AffineMapFE.scale("_Minus_Heading_A", "Heading_A", 2, -1.0f,
            new DistanceFE("_A_To_B", "Position_B", "Position_A", DistanceFE.DistanceType.VECTOR,
            new AngleBetweenVectorsFE("Aspect_A", "_Minus_Heading_A", "_A_To_B",

            // if Aspect_B is 0, A is behind B.  If 180, A is in front of B
            AffineMapFE.scale("_Minus_Heading_B", "Heading_B", 2, -1.0f,
            new DistanceFE("_B_To_A", "Position_A", "Position_B", DistanceFE.DistanceType.VECTOR,
            new AngleBetweenVectorsFE("Aspect_B", "_Minus_Heading_B", "_B_To_A",

//            new PrintObservationFE("",

            null))))))))))))));
    }

    @Override
    public MLFeatureExtractor getTrainingFE() throws Exception {
        return commonFeatures();
//        return commonFeatures().addToChain(new ConstantFE("Score", null, new Float[]{supervisedScore}, null));
    }

    @Override
    public MLFeatureExtractor getPredictionFE() throws Exception {
        return commonFeatures().addToChain(CogFoundryFE.createAlgorithm("Score", getPredictorNames(), algorithm, getTrainingData(), null));
    }

    @Override
    public MLFeatureExtractor getActionFE() throws Exception {
        return new BetweenFE("Score", 0.8f, null, new CreateEventFE(getResultTags(), false, true, null));
    }

    @Override
    public String[] getInputNames(Mode op) {
        return new String[] { "Position_A", "Position_B" };
    }

    @Override
    public String[] getDefaultPredictorNames() {
        return new String[] { "Range" };
    }
}
