/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.features;

import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.MLObservation;
import gov.sandia.rbb.ml.MLObservationSequence;

import java.util.ArrayList;


/**
 *
 * BufferObservationSequencesFE collects the MLObservation Sequences from all problem instances
 * into a ArrayList<MLObservationSequence> specified at construction.
 *
 * Multiple BufferObservationSequencesFE can be pointed to the same
 * ArrayList<MLObservationSequence> so they all collect to the same data structure.
 *
 * @author rgabbot
 */
public class BufferObservationsFE extends MLFeatureExtractor {

    protected ArrayList<MLObservationSequence> observationSequences;
    protected MLObservationSequence observationSequence;

    public BufferObservationsFE(ArrayList<MLObservationSequence> observationSequences, MLFeatureExtractor nextFE) {
        super(nextFE);
        this.observationSequences = observationSequences;
    }

    @Override
    public void init(double time, MLObservation.Metadata md) throws Exception {
        this.observationSequence = new MLObservationSequence(null, null, md);
        this.observationSequences.add(this.observationSequence);
        super.init(time, md);
    }

    @Override
    public void observe(MLObservation obs) throws Exception {
        this.observationSequence.addObservation(obs);
        super.observe(obs);
    }

}
