/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.sandia.rbb.ui;

import java.util.List;

public interface TagsetEditPanelModel {
    public int getTagCount();
    public String getTagName(int i);
    public List<String> getValues(int i);
}
