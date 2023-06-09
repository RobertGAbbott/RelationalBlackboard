/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;
import gov.sandia.rbb.RBB;

import java.util.Arrays;

/**
 * Implements an affine map - that is, y = Mx + b
 * where x is the input (a column vector), b is an additive constant vector,
 * M is a |b|*|x| matrix, and y is the output vector.
 *
 * Static factory methods construct common mapping mapping functions.
 *
 *
 *
 * @author rgabbot
 */
public class AffineMapFE extends MLFeatureExtractor {

    float[][] M;
    float[] b;

    private String input;
//    private boolean beforeFirstObservation;

    /**
     * constructs an identity function of the input
     * (input dimensions are unchanged, any additional output dimensions are 0)
     *
     */
    public AffineMapFE(String output, String input, int inputDim, int outputDim, MLFeatureExtractor nextFeatureExtractor)
    {
        super(nextFeatureExtractor, output);
 
        this.input = input;

        M = new float[outputDim][];
        for(int i = 0; i < outputDim; ++i)
            M[i] = new float[inputDim];
        b = new float[outputDim];

        for(int i = 0; i < inputDim && i < outputDim; ++i)
            M[i][i] = 1.0f;
    }

    /*
     * Constructor linear function of a 1d input.
     */
    public AffineMapFE(String output, String input, Float m, Float b, MLFeatureExtractor nextFeatureExtractor)
    {
        super(nextFeatureExtractor, output);
        this.input = input;
        M = new float[][]{new float[]{m}};
        this.b = new float[]{b};
    }

    @Override
    public AffineMapFE clone() throws CloneNotSupportedException {
        AffineMapFE result = (AffineMapFE) super.clone();
        result.M = new float[M.length][];
        for(int i = 0; i < M.length; ++i)
            result.M[i] = Arrays.copyOf(M[i], M[i].length);
        result.b = Arrays.copyOf(b, b.length);
        return result;
    }

    /**
     * Create an affine map that scales each dimension of the input by the same factor.
     */
    public static AffineMapFE scale(String output, String input, int dim, float s, MLFeatureExtractor nextFeatureExtractor) {
        AffineMapFE map = new AffineMapFE(output, input, dim, dim, nextFeatureExtractor);
        for(int i = 0; i < dim; ++i)
            map.M[i][i] = s;
        return map;
    }

    /**
     * Create a 1-d output by selecting a single column of the input, i, which is 0-based.
     */
    public static AffineMapFE selectDim(String output, String input, int inputDim, int which, MLFeatureExtractor nextFeatureExtractor) {
        AffineMapFE map = new AffineMapFE(output, input, inputDim, 1, nextFeatureExtractor);
        for(int i = 0; i < which; ++i)
            map.M[0][i] = 0.0f;
        map.M[0][which] = 1.0f;
        for(int i = which+1; i < inputDim; ++i)
            map.M[0][i] = 0.0f;

        return map;
    }

    /**
     * create a rotation and translation that maps the input relative to some other time-varying position and orientation.
     */
    public static AffineMapFE relativePosition(String outputPosition, String inputPosition,
            final String referencePosition, final String referenceHeading,
            int dim, MLFeatureExtractor nextFeatureExtractor) {
        return new AffineMapFE(outputPosition, inputPosition, dim, dim, nextFeatureExtractor)
        {
            @Override public void observe(MLObservation obs) throws Exception {
                super.relativeTo(obs, referencePosition, referenceHeading);
                super.observe(obs);
            }
        };
    }


    /**
     * inverse of standardOrientation mapping.
     */
    public static AffineMapFE inverseRelativePosition(String outputPosition, String inputPosition, 
            final String referencePosition, final String referenceOrientation,
            int dim, MLFeatureExtractor nextFeatureExtractor) {
        return new AffineMapFE(outputPosition, inputPosition, dim, dim, nextFeatureExtractor)
        {
            @Override public void observe(MLObservation obs) throws Exception {
                super.inverseOfRelativeTo(obs, referencePosition, referenceOrientation);
                super.observe(obs);
            }
        };
    }

    @Override
    public void observe(MLObservation obs) throws Exception {
        Float[] x = obs.getFeatureAsFloats(input);

        if(x != null) {
            Float[] y = new Float[b.length];
            for(int row = 0; row < y.length; ++row) {
                y[row] = b[row];
                for(int col = 0; col < M[row].length; ++col)
                    y[row] += M[row][col] * x[col];
            }
            obs.setFeatureAsFloats(this.getOutputName(0), y);
        }

        super.observe(obs);
    }

    private static float length(Float[] x) {
        double rsq = 0.0;
        for(Float x0 : x)
            rsq += x0*x0;
        return (float) Math.sqrt(rsq);
    }

    private static Float[] scale(Float[] x, float s) {
        Float[] y = new Float[x.length];
        for(int i = 0; i < x.length; ++i)
            y[i] = x[i]*s;
        return y;
    }

    private static Float[] unitLength(Float[] x) {
        return scale(x, 1.0f/length(x));
    }

    private void relativeTo(MLObservation obs, String position, String heading) {
        if(M.length != 2 || M[0].length != 2)
            throw new UnsupportedOperationException("AffineMapFE.relativeTo is only implemnted for dim=2");

        final Float[] h = obs.getFeatureAsFloats(heading);
        final Float[] x = obs.getFeatureAsFloats(position);
        if(h == null || x == null)
            return;
        final Float[] v = unitLength(h);

        M[0][0] = v[0];
        M[1][0] = -v[1];
        M[0][1] = v[1];
        M[1][1] = v[0];
        b[0] = -(M[0][0]*x[0]+M[0][1]*x[1]);
        b[1] = -(M[1][0]*x[0]+M[1][1]*x[1]);
    }

    private void inverseOfRelativeTo(MLObservation obs, String position, String heading) {
       if(M.length != 2 || M[0].length != 2)
            throw new UnsupportedOperationException("AffineMapFE.inverseOfRelativeTo is only implemnted for dim=2");


        final Float[] h = obs.getFeatureAsFloats(heading);
        final Float[] x = obs.getFeatureAsFloats(position);
        if(h == null || x == null)
            return;
        final Float[] v = unitLength(h);

        M[0][0] = v[0];
        M[0][1] = -v[1];
        M[1][0] = v[1];
        M[1][1] = v[0];
        b[0] = x[0];
        b[1] = x[1];
    }
}
