/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.cognition.math.matrix.Vector;
import gov.sandia.cognition.math.matrix.mtj.DenseVector;
import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;
import gov.sandia.rbb.RBB;
import java.util.ArrayList;
import static gov.sandia.rbb.ml.features.CogFoundryFE.arrayToVector;
import static gov.sandia.rbb.ml.features.CogFoundryFE.vectorToFloats;


/**
 *
 * Project the closest point of approach for two points A and B assuming constant velocity for each.
 * Various different outputs can be created; see constructor for more detail.
 *
 * If A and B have equal velocity the time of CPA is arbitrarily chosen to be right now.
 *
 * @author rgabbot
 */
public class ClosestPointOfApproachFE extends MLFeatureExtractor {

    private String positionA, positionB, velocityA, velocityB;

    static final gov.sandia.cognition.math.matrix.VectorFactory<DenseVector> vectorFactory = gov.sandia.cognition.math.matrix.mtj.DenseVectorFactoryMTJ.INSTANCE;

    private String distanceAtCPA;
    private String timeUntilCPA;
    private String positionAatCPA;
    private String positionBatCPA;
    private int numNearest;

    /**
     *
     * @param distanceAtCPA: output: projected minimum distance from position A to position B.  May be null.
     * @param timeUntilCPA: output: projected time remaining until CPA.  Will be negative if a and b are moving apart.  May be null.
     * @param positionAatCPA: output: projected position of A at time of CPA.  May be null.
     * @param positionBatCPA: output: projected position of B at time of CPA.  May be null.
     * @param positionA: input: current position A
     * @param velocityA: input: current velocity A.  May be null, in which case A is presumed stationary.
     * @param positionB: input: current position B
     * @param velocityB: input: current velocity B.  May be null in which case B is presumed stationary.
     * @param nextFeatureExtractor
     */
    public ClosestPointOfApproachFE(
            String distanceAtCPA,
            String timeUntilCPA,
            String positionAatCPA,
            String positionBatCPA,
            String positionA, String velocityA,
            String positionB, String velocityB,
            MLFeatureExtractor nextFeatureExtractor)
    {
        super(nextFeatureExtractor, nonNullsAsArray(distanceAtCPA, timeUntilCPA, positionAatCPA, positionBatCPA));

        this.distanceAtCPA = distanceAtCPA;
        this.timeUntilCPA = timeUntilCPA;
        this.positionAatCPA = positionAatCPA;
        this.positionBatCPA = positionBatCPA;

        this.positionA = positionA;
        this.positionB = positionB;
        this.velocityA = velocityA;
        this.velocityB = velocityB;
    }

    private static String[] nonNullsAsArray(String... a) {
        ArrayList<String> result = new ArrayList<String>();
        for(String s : a)
            if(s != null)
                result.add(s);
        return result.toArray(new String[0]);
    }

    public static class Result {
        public Float distanceAtCPA;
        public Float timeUntilCPA;
        Float[] positionAatCPA;
        Float[] positionBatCPA;
    }

    /*
     * This is the function that actually computes the CPA.
     * Either or both velocities may be null in which case the 0 vector is assumed.
     */
    public static Result CPA(Float[] positionA, Float[] velocityA, Float[] positionB, Float[] velocityB, boolean allowNegativeTime) {

        // the time when positionA comes closest to positionB is the same as when
        // positionA relative to positionB comes closest to the origin.

        Vector pA = arrayToVector(positionA);
        Vector pB = arrayToVector(positionB);
        Vector vA = velocityA == null ? vectorFactory.createVector(pA.getDimensionality(), 0.0) : arrayToVector(velocityA);
        Vector vB = velocityB == null ? vectorFactory.createVector(pB.getDimensionality(), 0.0) : arrayToVector(velocityB);

        Vector p = pA.minus(pB);
        Vector v = vA.minus(vB);

        double t;
        if(v.norm1()<1e-6) { // iszero has a bug: it returns true if y component is negative?
//            System.err.println("No realative motion: "+v+" "+ vA +" " + vB + " " + velocityA + " " + velocityB);
            t = 0.0;
        }
        else
            t = -p.dotProduct(v) / v.dotProduct(v);

        if(!allowNegativeTime)
            t = Math.max(0, t);

        plusEquals(pA, vA, t); // now pA is predicted position of A at CPA
        plusEquals(pB, vB, t); // now pB is predicted position of B at CPA

        // store and return the result.
        Result result = new Result();
        result.distanceAtCPA = (float) pA.euclideanDistance(pB);
        result.timeUntilCPA = (float) t;
        result.positionAatCPA = vectorToFloats(pA);
        result.positionBatCPA = vectorToFloats(pB);

        return result;
    }

    @Override
    public void observe(MLObservation obs) throws Exception {

        Result r = CPA(obs.getFeatureAsFloats(positionA), velocityA==null ? null : obs.getFeatureAsFloats(velocityA),
                       obs.getFeatureAsFloats(positionB), velocityB==null ? null : obs.getFeatureAsFloats(velocityB), true);

        if(timeUntilCPA != null)
            obs.setFeatureAsFloats(timeUntilCPA, r.timeUntilCPA);

        if(distanceAtCPA != null)
            obs.setFeatureAsFloats(distanceAtCPA, r.distanceAtCPA);

        if(positionAatCPA != null)
            obs.setFeatureAsFloats(positionAatCPA, r.positionAatCPA);

        if(positionBatCPA != null)
            obs.setFeatureAsFloats(positionBatCPA, r.positionBatCPA);

        super.observe(obs);
    }

    /**
     * a += b*s
     */
    private static void plusEquals(Vector a, Vector b, double s) {
        for(int i = 0; i < a.getDimensionality(); ++i)
            a.setElement(i, a.getElement(i)+s*b.getElement(i));
    }

}
