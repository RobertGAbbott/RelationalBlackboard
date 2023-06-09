
package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;
import gov.sandia.rbb.ml.MLObservation.Metadata;
import gov.sandia.rbb.ml.MLObservationSequence;

/**
 *
 * TrajectoryFE accrues the full history of another feature,
 * resamples it to a specified fixed length,
 * and then creates the output feature by concatenating all the past values together.
 *
 * It has the same value at every time, which is the full resampled concatenated input feature.
 *
 * It accomplishes this with an unspecified warmup and cooldown, so NO observations
 * will be done until after the final observation.  So it is not suitable for live processing.
 *
 * When resampling to fixed length, the first and last observations are used,
 * then others equally spaced inbetween.
 *
 *
 * @author rgabbot
 */
public class TrajectoryFE extends MLFeatureExtractor {

    MLObservationSequence allObs;
    Integer fixedLength = null;
    String input = null;
    Double start;

    public TrajectoryFE(String output, String input, int fixedLength, MLFeatureExtractor nextFeatureExtractor) {
        super(nextFeatureExtractor, output);
        if(fixedLength < 2)
            throw new IllegalArgumentException("TrajectoryFE Error: fixedLength parameter must be >= 2");
        this.fixedLength = fixedLength;
        this.input = input;
    }

    @Override
    public void init(double time, Metadata md) throws Exception {
        allObs = new MLObservationSequence(null, null, md);
        start = time;
    }

    @Override
    public void observe(MLObservation obs) throws Exception {
        allObs.addObservation(obs);
    }

    @Override
    public void done(double time) throws Exception {
        final double end = time;

        // make one vector of the entire history of the specified feature.

        Float[] x = new Float[allObs.getOldest().getFeatureAsFloats(input).length*fixedLength];
        int dst=0;
        for(int i = 0; i < fixedLength; ++i) {
            Float[] src = allObs.getOldest(i * (allObs.size()-1) / (fixedLength-1)).getFeatureAsFloats(input);
            for(int j = 0; j < src.length; ++j)
                x[dst++] = src[j];
        }

        super.init(start, allObs.getMetadata());
        for(int i = 0; i < allObs.size(); ++i) {
            MLObservation obs = allObs.getOldest(i);
            obs.setFeature(super.getOutputName(0), x);
            super.observe(obs);
            allObs.getOldest(i);
        }
        super.done(end);
    }

    @Override
    public Double getCooldown() {
        return null;
    }

    @Override
    public Double getWarmup() {
        return null;
    }


}
