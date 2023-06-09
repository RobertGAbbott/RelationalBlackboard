
package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;
import gov.sandia.rbb.RBB;


/**
 * Project a location behind another point (the leader), given the leader's position and velocity.
 */
public class FollowerFE extends MLFeatureExtractor {

    private String outputPosition;
    private String leaderPosition;
    private String leaderVelocity;
    float lag;
    
    public FollowerFE(String outputPosition, String leaderPosition, String leaderVelocity, float lag, MLFeatureExtractor nextFeatureExtractor)
    {
        super(nextFeatureExtractor, outputPosition);

        this.outputPosition = outputPosition;
        this.leaderPosition = leaderPosition;
        this.leaderVelocity = leaderVelocity;

        this.lag = lag;
    }

    @Override
    public void observe(MLObservation obs) throws Exception {
        Float[] x = obs.getFeatureAsFloats(leaderPosition);
        Float[] v = obs.getFeatureAsFloats(leaderVelocity);

        Float[] y = new Float[x.length];

        for(int i = 0; i < x.length; ++i)
            y[i] = x[i]-v[i]*lag;

        obs.setFeatureAsFloats(this.getOutputName(0), y);

        super.observe(obs);
    }
}
