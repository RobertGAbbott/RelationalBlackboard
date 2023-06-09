/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author rgabbot
 */
public class MLFeatureExtractorTest {

    @Test
    public void testWarmupCooldown() throws Exception {
        MLFeatureExtractor fe = new MLFeatureExtractor(null, new String[]{});

        assertEquals(0.0, fe.getMaxWarmup(), 1e-8);
        assertEquals(0.0, fe.getMaxCooldown(), 1e-8);

        fe.addToChain(new MLFeatureExtractor(null, new String[]{}) {
            @Override public Double getWarmup() { return 1.0; };
            @Override public Double getCooldown() { return 2.0; };
        });
        assertEquals(1.0, fe.getMaxWarmup(), 1e-8);
        assertEquals(2.0, fe.getMaxCooldown(), 1e-8);

        fe.addToChain(new MLFeatureExtractor(null, new String[]{}) {
            @Override public Double getWarmup() { return 0.5; };
            @Override public Double getCooldown() { return 0.5; };
        });
        assertEquals(1.0, fe.getMaxWarmup(), 1e-8);
        assertEquals(2.0, fe.getMaxCooldown(), 1e-8);

        fe.addToChain(new MLFeatureExtractor(null, new String[]{}) {
            @Override public Double getWarmup() { return null; };
        });
        assertNull(fe.getMaxWarmup());
        assertEquals(2.0, fe.getMaxCooldown(), 1e-8);

        fe.addToChain(new MLFeatureExtractor(null, new String[]{}) {
            @Override public Double getCooldown() { return null; };
        });
        assertNull(fe.getMaxWarmup());
        assertNull(fe.getMaxCooldown());

        fe.addToChain(new MLFeatureExtractor(null, new String[]{}) {
            @Override public Double getWarmup() { return 0.5; };
            @Override public Double getCooldown() { return 0.5; };
        });
        assertNull(fe.getMaxWarmup());
        assertNull(fe.getMaxCooldown());
    }
}
