package gov.sandia.rbb.ui;

import gov.sandia.rbb.Tagset;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.ScrollPaneConstants;
import replete.gui.uidebug.DebugPanel;
import replete.gui.uidebug.UIDebugUtil;
import replete.util.GUIUtil;

public class TagsetEditPanel extends JPanel {
    private static final String ANY_ITEM = "ANY";

    private TagsetEditPanelModel model;
    private List<JCheckBox> checkBoxes = new ArrayList<JCheckBox>();
    private List<JComboBox> comboBoxes = new ArrayList<JComboBox>();

    public TagsetEditPanel(TagsetEditPanelModel m) {
        model = m;
        setBackground(Color.red);
        setPreferredSize(new Dimension(200, 200));
        JPanel pnlInner = new DebugPanel();
        //pnlInner.setPreferredSize(new Dimension(400, 400));
        BoxLayout b = new BoxLayout(pnlInner, BoxLayout.Y_AXIS);
        pnlInner.setLayout(b);
        for(int ts = 0; ts < model.getTagCount(); ts++) {
//
//            JLabel lbl = new DebugLabel("<html>"+animals[x] + "</html>");
//            Dimension d = GUIUtil.getHTMLJLabelPreferredSize(lbl, 100, true);
//            lbl.setPreferredSize(d);

            JCheckBox chk = new JCheckBox("<html>"+model.getTagName(ts) + "</html>");
            Dimension d2 = GUIUtil.getHTMLJLabelPreferredSize(chk, 100, true);
            chk.setPreferredSize(d2);

            DefaultComboBoxModel mdl = new TagValuesComboBoxModel(model, ts);
            JComboBox cbo = new JComboBox(mdl);
            cbo.setPreferredSize(new Dimension(150, 25));
            cbo.setMaximumSize(new Dimension(150, 25));
            //cbo.setEnabled(false);

            JPanel pnlRow = new DebugPanel();
            BoxLayout bb = new BoxLayout(pnlRow, BoxLayout.X_AXIS);
            pnlRow.setLayout(bb);
            //pnlRow.setLayout(new FlowLayout());
            //pnlRow.setPreferredSize(new Dimension(1, 1));
//            pnlRow.add(lbl);
            //pnlRow.add(chk);
            pnlRow.add(new JLabel("<html>"+model.getTagName(ts) + "</html>"));
            pnlRow.add(cbo);
            pnlInner.add(pnlRow);
            pnlRow.setBorder(BorderFactory.createEmptyBorder(5, 5, 0, 5));

            checkBoxes.add(chk);
            comboBoxes.add(cbo);

            /*chk.addItemListener(new ItemListener() {
                public void itemStateChanged(ItemEvent e) {
                    JCheckBox chk = (JCheckBox) e.getSource();
                    JComboBox cbo = comboBoxes.get(checkBoxes.indexOf(chk));
                    cbo.setEnabled(chk.isSelected());
                }
            });*/
        }
        pnlInner.add(Box.createVerticalGlue());

        setLayout(new BorderLayout());
        add(new JScrollPane(pnlInner,
            ScrollPaneConstants.VERTICAL_SCROLLBAR_AS_NEEDED,
            ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED),
            BorderLayout.CENTER);
    }

    public Tagset getSelectedTagset() {
        String s = "";
        for(int ts = 0; ts < model.getTagCount(); ts++) {
            if(checkBoxes.get(ts).isSelected()) {
                s += model.getTagName(ts);
                JComboBox cbo = comboBoxes.get(ts);
                String item = (String) cbo.getSelectedItem();
                if(!item.equals(ANY_ITEM)) {
                   s += "=" + item;
                }
                s += ",";
            }
        }
        if(s.length() != 0) {
            s = s.substring(0, s.length() - 1);
        }

        return new Tagset(s);
    }

    public void setSelectedTagset(Tagset tagset) {
        for(int ts = 0; ts < model.getTagCount(); ts++) {
            if(tagset.containsName(model.getTagName(ts))) {
                checkBoxes.get(ts).setSelected(true);
                String v = tagset.getValue(model.getTagName(ts));
                if(!model.getValues(ts).contains(v)) {
                    comboBoxes.get(ts).setSelectedItem(ANY_ITEM);
                } else {
                    comboBoxes.get(ts).setSelectedItem(v);
                }
            } else {
                checkBoxes.get(ts).setSelected(false);
            }
        }
    }

    public static void main(String[] args) {

        JFrame f = new JFrame();
        f.setLayout(new BorderLayout());

//        UIDebugUtil.enableColor();
        f.add(new DebugPanel(), BorderLayout.CENTER);

        JPanel west = new DebugPanel();
        west.setLayout(new BorderLayout());
        f.add(west, BorderLayout.WEST);
        final TagsetEditPanel tep = new TagsetEditPanel(new TestModel());
        JButton btn = new JButton("SetSelTS");
        btn.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                String s = JOptionPane.showInputDialog("tagset!");
                if(s != null) {
                    tep.setSelectedTagset(new Tagset(s));
                }
            }
        });
        
        west.add(btn, BorderLayout.CENTER);
        west.add(tep, BorderLayout.SOUTH);

        f.setSize(800, 600);
        f.setLocationRelativeTo(null);
        f.setVisible(true);
    }

    private class TagValuesComboBoxModel extends DefaultComboBoxModel {
        public TagsetEditPanelModel panelModel;
        public int whichKey;
        public TagValuesComboBoxModel(TagsetEditPanelModel pm, int which) {
            panelModel = pm;
            whichKey = which;
            setSelectedItem(ANY_ITEM);
        }
        @Override
        public int getSize() {
            return panelModel.getValues(whichKey).size() + 1;
        }
        @Override
        public Object getElementAt(int index) {
            if(index == 0) {
                return ANY_ITEM;
            }
            return panelModel.getValues(whichKey).get(index - 1);
        }
        @Override
        public void setSelectedItem(Object a) {
            if(panelModel.getValues(whichKey).contains((String) a)) {
                super.setSelectedItem(a);
            } else {
                super.setSelectedItem(ANY_ITEM);
            }
        }
    }
    
    public static class TestModel implements TagsetEditPanelModel {
        String[] animals = {"Tiger and Jerry the 5th and the hatter mad", "Lion", "Bear"};

        public int getTagCount() {
            return 3;
        }
        public String getTagName(int i) {
            return animals[i];
        }
        public List<String> getValues(int i) {
            List<String> values = new ArrayList<String>();
            values.add("one");
            values.add("two");
            values.add("three");
            return values;
        }
    }
}
