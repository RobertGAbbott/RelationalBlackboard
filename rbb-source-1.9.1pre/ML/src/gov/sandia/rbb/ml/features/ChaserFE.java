/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;
import gov.sandia.rbb.RBB;
import gov.sandia.rbb.ml.MLObservation.Metadata;


/**
 * Chaser is a very simple 2d physical simulation of an particle that
 * 'chases' its input feature.  The maximum acceleration, velocity and rotational velocity are bounded.
 *
 * Example invocation for an 'entity' that chases the mouse in DrawTimeseries (set "Create With" to "type=mouse" in draw):
 *
 * $RBB ml getFeatures $S -online -input leader type=mouse -ChaserFE chaser:leader:100:200:0.25:2 -PrintFeatureFE "":chaser -CreateTimeseriesFE type=position,color=green:chaser:true:true
 *
 * @author rgabbot
 */
public class ChaserFE extends MLFeatureExtractor {

    private String input;

    Float[] position;
    Double speed;
    Double heading;
    MLObservation prevObservation;

            
    Float maxAcceleration, maxSpeed, maxTurnRate, desiredFollowingDistance;

    /**
     *
     * @param output: name of feature created by this feature extractor
     * @param input: name of the feature that specifies the leader position
     * @param maxAcceleration: per unit of time^2
     * @param maxSpeed: max unit of distance per unit of time
     * @param maxTurnRate: In units of revolutions per unit time
     * @param desiredFollowingDistance: will try to be where the leader is now after this many units of time.
     * @param nextFeatureExtractor
     */
    public ChaserFE(String output, String input, float maxAcceleration, float maxSpeed, float maxTurnRate, float desiredFollowingDistance, MLFeatureExtractor nextFeatureExtractor)
    {
        super(nextFeatureExtractor, output);

        this.input = input;
        this.maxAcceleration = maxAcceleration;
        this.maxSpeed = maxSpeed;
        this.maxTurnRate = maxTurnRate;
        this.desiredFollowingDistance = desiredFollowingDistance;
    }

    @Override
    public void init(double time, Metadata md) throws Exception {
        super.init(time, md);

        prevObservation = null;
        heading = speed = null;
    }

    private double normalizeRadians(double x) {
        while(x < -Math.PI)
            x += 2 * Math.PI;
        while(x > Math.PI)
            x -= 2 * Math.PI;
        return x;
    }

    @Override
    public void observe(MLObservation obs) throws Exception {
        Float[] leaderPos = obs.getFeatureAsFloats(input);

        if(prevObservation==null) {
            position = new Float[]{leaderPos[0], leaderPos[1]};
        }
        else {
            position = new Float[]{position[0], position[1]}; // must allocate a new fature for each successive observation
            final float dt = (float)(obs.getTime()-prevObservation.getTime());

            if(heading==null) { // this is only the second observation.
                Float[] leaderPrevPos = prevObservation.getFeatureAsFloats(input);
                heading = Math.atan2((double)(leaderPos[1]-leaderPrevPos[1]), (double)(leaderPos[0]-leaderPrevPos[0]));
                speed = 0.0;
            }
            else {
                double desiredHeading = Math.atan2((double)(leaderPos[1]-position[1]), (double)(leaderPos[0]-position[0]));
                double desiredTurn = desiredHeading - heading;

                desiredTurn = normalizeRadians(desiredTurn);


                float maxTurn = dt * maxTurnRate*(float)(2.0*Math.PI);

                double turn = Math.signum(desiredTurn) * Math.min(maxTurn, Math.abs(desiredTurn));
                
                heading += turn;

                heading = normalizeRadians(heading);

                // System.err.println("DesiredTurn = " + desiredTurn + "; turn = " + turn + "; heading = " + heading);

            }

            // my desired speed (velocity magnitude) is such that, if I were currrently moving straight
            // toward the leader, I would arrive at its current location in 'desiredFollowingDistance' seconds.
            double desiredSpeed = DistanceFE.distance(leaderPos, position) / desiredFollowingDistance;
            double desiredAcceleration = desiredSpeed - speed;
            double acceleration;
            if(desiredAcceleration < 0.0f) // braking.  Can decelerate at the same rate from any speed.
                acceleration = -Math.min(maxAcceleration, -desiredAcceleration);
            else
                acceleration = Math.min(desiredAcceleration,  maxAcceleration * (1.0f-speed/maxSpeed));
            speed += dt * acceleration;

            position[0] += (float) (dt*speed*Math.cos(heading));
            position[1] += (float) (dt*speed*Math.sin(heading));
        }

        obs.setFeatureAsFloats(this.getOutputName(0), position);

        prevObservation = obs;

        super.observe(obs);
    }

}
