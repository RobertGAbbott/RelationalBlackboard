/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.sandia.rbb.ml.models;

import gov.sandia.rbb.ml.MLModel;
import gov.sandia.rbb.ml.MLFeatureExtractor;
import gov.sandia.rbb.ml.RBBML.Mode;
import static gov.sandia.rbb.ml.RBBML.Mode.*;

/**
 * A TAFOneEntityModel is a fixed path carried out as a function of time.
 * It is translated and rotated per the initial position and heading of the entity.
 *
 * S=jdbc:h2:tcp:localhost//tmp/Session
 * M=jdbc:h2:tcp:localhost//tmp/Model
 * perl -e 'print "color=red,type=drawing\n"; for($t=0; $t <= 5; $t+=0.2) {  print "$t,",($t*40),",",($t*10),"\n"}' | $RUNRBB put $S
 *
 * $RUNML ml create $M gov.sandia.rbb.ml.taf.TAFOneEntityModel Path
 * $RUNML ml apply $S $M color=red
 * $RUNML ml train $S $M 1 3.5
 * 
 * (echo test=draw,color=green; $RUNML ml print $S $M | perl -e 'while(<>){ last if /TRAINING_EVENT/} while(<>){ last if /RBBML/; ($x,$t)=/\((.*?)\)\s+\((.*)\)/; print "$t,$x\n"}') | $RUNRBB put $S
 */
public class TAFOneEntityModel extends MLModel {

    @Override
    public String[] getInputNames(Mode op) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String[] getDefaultPredictorNames() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public MLFeatureExtractor getPredictionFE() throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public MLFeatureExtractor getTrainingFE() throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }


}
