/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;

/**
 *
 * @author rgabbot
 */
public class TagValueFE extends MLFeatureExtractor {
    String featureName;
    String tagName;

    public TagValueFE(String output, String featureName, String tagName, MLFeatureExtractor nextFeatureExtractor)
    {
        super(nextFeatureExtractor, output);
        this.featureName = featureName;
        this.tagName = tagName;
    }

    @Override
    public void observe(MLObservation obs) throws Exception {
        obs.setFeature(super.getOutputName(0), obs.getFeatureEvent(featureName).getTagset().getValue(tagName));
        super.observe(obs);
    }


}
