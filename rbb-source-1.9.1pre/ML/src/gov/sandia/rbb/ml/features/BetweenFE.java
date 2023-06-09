/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;


/**
 * The BetweenFE evaluates its successor in the chain whenever the
 * input feature is in the range min to max, inclusive.
 *<p>
 * Min or max may be null, and then equal -infinity to infinity
 *<p>
 * If the inputFeature is null the test never succeeds, regardless of min/max.
 *
 */
public class BetweenFE extends SegmentationFE
{
    private Float minScore;
    private Float maxScore;
    private String inputFeatureName;

    public BetweenFE(String inputFeatureName, Float min, Float max, MLFeatureExtractor next)
    {
        super(next);
        this.inputFeatureName = inputFeatureName;
        this.minScore = min;
        this.maxScore = max;
    }
    
    @Override
    public boolean gateTest(MLObservation obs) throws Exception {
        Float[] v = obs.getFeatureAsFloats(inputFeatureName);
        if(v==null)
            return false;
        Float x = v[0];
        if(minScore != null && x < minScore)
            return false;
        if(maxScore != null && x > maxScore)
            return false;
        return true;
    }
}
