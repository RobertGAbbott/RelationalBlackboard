/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.models;

import gov.sandia.rbb.ml.MLModel;
import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.RBBML.Mode;
import static gov.sandia.rbb.ml.RBBML.Mode.*;
import gov.sandia.rbb.ml.features.*;

/**
 * Implementation of MLModel for pairs of positions.
 *
 */
public class OneEntityModel extends MLModel {
    private float supervisedScore = 0.0f;

    @Override
    public void parseTrainingArgs(String... args) throws Exception {
        supervisedScore = Float.parseFloat(args[0]);
    }

    // returns the part of the feature extraction chain common
    // to training and prediction.
    private MLFeatureExtractor commonFeatures() throws Exception {
        return new RateFE("_Heading", "Position",
            new SmoothingFE("Heading", "_Heading", 0.5, 0.5,
            null));
    }

    @Override
    public MLFeatureExtractor getTrainingFE() throws Exception {
        return commonFeatures().addToChain(new ConstantFE("Score", null, new Float[]{supervisedScore}, null));
    }

    @Override
    public MLFeatureExtractor getPredictionFE() throws Exception {
        return commonFeatures().addToChain(CogFoundryFE.createDefault("Score", getPredictorNames(), getTrainingData(), null));
    }

    @Override
    public String[] getInputNames(Mode op) {
        return new String[] { "Position" };
    }

    @Override
    public String[] getDefaultPredictorNames() {
        return new String[] { "Position" };
    }


}
