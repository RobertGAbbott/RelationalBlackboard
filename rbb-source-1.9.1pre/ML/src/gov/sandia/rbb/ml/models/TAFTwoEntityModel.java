/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.models;

import gov.sandia.rbb.Tagset;
import gov.sandia.rbb.ml.MLModel;
import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.RBBML.Mode;
import static gov.sandia.rbb.ml.RBBML.Mode.*;
import gov.sandia.rbb.ml.features.*;
import gov.sandia.rbb.util.StringsWriter;

/**
 *
 * cd ~/workspace/Spatial/RelationalBlackboard; . ./environment; S=jdbc:h2:tcp:localhost//tmp/Session; M=jdbc:h2:tcp:localhost//tmp/Model
 * perl -e 'print "color=red,type=drawing\n"; for($t=0; $t <= 5; $t+=0.2) {  print "$t,",($t*40),",",($t*10),"\n"}' | $RUNRBB put $S
 *
 $RUNML ml predict $S $M type=position,platform=predator -store type=TAF -online -120 50
 *
 * (echo test=draw,color=green; $RUNML ml print $S $M | perl -e 'while(<>){ last if /TRAINING_EVENT/} while(<>){ last if /RBBML/; ($x,$t)=/\((.*?)\)\s+\((.*)\)/; print "$t,$x\n"}') | $RUNRBB put $S
 *
 *
 * The TAFTwoEntityModel models the position of self relative to
 * the current and past positions of a second entity.
 *
 */
public class TAFTwoEntityModel extends MLModel {

    @Override
    public String[] getInputNames(Mode op) {
        if(op == TRAINING)
            return new String[] { "OtherPosition", "MyPosition" };
        else // op == PREDICTION; I am not fed my own position.
            return new String[] { "OtherPosition" };
    }

    @Override
    public String getPredictionName() {
        // this is overridden because the prediction, MyNextRelativePosition, is not the final output... MyNextPosition is.
        return "MyNextRelativePosition";
    }

    /*
     * This is the part of the feature extraction chain common
     * to both training and prediction.
     */
    private MLFeatureExtractor commonFeatures() {
        return new RateFE("_OtherVelocity", "OtherPosition",
            new SmoothingFE("OtherVelocity", "_OtherVelocity", 0.5, 0.0,
            new RateFE("OtherAcceleration", "OtherVelocity",
            new TurnRateFE("_OtherTurnRate", "OtherPosition",
            new SmoothingFE("OtherTurnRate", "_OtherTurnRate", 0.5, 0.0,
            AffineMapFE.relativePosition("MyRelativePosition", "MyPosition", "OtherPosition", "OtherVelocity", 2, null))))));

    }

    @Override
    public MLFeatureExtractor getTrainingFE() throws Exception {
        return commonFeatures().addToChain(
                new FutureValueFE(getPredictionName(), "MyRelativePosition", 1, null));
                // AffineMapFE.inverseRelativePosition("MyNextPosition", "MyNextRelativePosition", "OtherPosition", "OtherVelocity", 2, null)));
    }

    @Override
    public MLFeatureExtractor getPredictionFE() throws Exception {
        MLFeatureExtractor fe = new FeedbackFE("MyPosition", "MyNextPosition", initialPosition, null);
        fe.addToChain(commonFeatures());
        fe.addToChain(CogFoundryFE.createDefault(getPredictionName(), getPredictorNames(), getTrainingData(), null));
        fe.addToChain(AffineMapFE.inverseRelativePosition("MyNextPosition", getPredictionName(), "OtherPosition", "OtherVelocity", 2, null));
        return fe;
    }

    @Override
    public MLFeatureExtractor getActionFE() throws Exception {
        return new CreateTimeseriesFE(getResultTags(), "MyNextPosition", false, true, null);
    }



    @Override
    public String[] getDefaultPredictorNames() {
        return new String[] { "MyRelativePosition" };
    }

    @Override
    public void parsePredictionArgs(String... args) throws Exception {
        if(args.length != 2)
            throw new Exception("TAFTwoEntityModel.parsePredictionArgs: got " + args.length + " args ("+StringsWriter.join(" ",args)+
                    ").  Required: <x> <y> - initial position of new entity relative to the other.");
        initialPosition =  new Float[] { Float.parseFloat(args[0]), Float.parseFloat(args[1]) };
    }

    @Override
    public Double getTimestep() {
        return 0.25;
    }

    public static String usage = 
            "TAFTwoEntityModel <sessionRBB> <modelRBB> <tagsOfOtherEntity> <myTags> <x> <y>\n"+
            "   x y specifies my initial position relative to the other entity.";

    /**
     * The initial position of the entity created by this Model.
     */
    Float[] initialPosition;

    /**
     * Tagset for entities created by this model.
     */
    Tagset myTags;

}
