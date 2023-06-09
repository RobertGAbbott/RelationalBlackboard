/*
 * File:                RBBEventSpecDialog.java
 * Authors:             Charles Gieseler, Matt Glickman
 * Company:             Sandia National Laboratories
 * Project:             AEMASE Timeline
 *
 */

package gov.sandia.rbb.ui.timeline;

import gov.sandia.rbb.Event;
import gov.sandia.rbb.RBB;
import gov.sandia.rbb.Tagset;
import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

/**
 * A dialog to allow users to edit the text field and time of an annotation.
 * @author elgoodm
 *
 */
@SuppressWarnings("serial")
public class RBBEventSpecDialog
    extends JDialog
{

    protected static final JLabel TIME_LABEL = new JLabel("Start Time");

    protected static final JLabel ENDTIME_LABEL = new JLabel("End Time");

    protected static final JLabel TAGS_LABEL = new JLabel("Tags");

    protected static final String SAVE_BUTTON_TEXT = "Save";

    protected static final String CANCEL_BUTTON_TEXT = "Cancel";

    /** The author field used for editing in the dialog */
    protected JTextField timeField = new JTextField();

    /** The duration field used for editing in the dialog */
    protected JTextField endtimeField = new JTextField();

    /** The tags field used for editing in the dialog */
    protected JTextField tagsField = new JTextField();

    protected JButton saveButton = new JButton(SAVE_BUTTON_TEXT);

    protected JButton cancelButton = new JButton(CANCEL_BUTTON_TEXT);

    protected RBB rbb;

    public RBBEventSpecDialog(RBB theRbb,
        long currentTime)
    {
        rbb = theRbb;
        createGUI(currentTime);
        this.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
    }

    protected void createGUI(Long currentTime)
    {

        System.err.println("in edit dialogue creation");
        //String text = data.getText();
        //String author = data.getAuthor();
        //String type = data.getType();
        //long duration = data.getDuration();
        //long time = data.getTime();

        this.setLayout(new BorderLayout());

        //Adding field information
        //authorField.setText(author);

        //TODO: Account for start time offset to be able to convert to relative
        // time rather than absolute time.
        timeField.setText(currentTime.toString());
        endtimeField.setText(currentTime.toString());
        //noteText.setText(text);
        //typeField.setText(type);
        TextFieldKeyListener fieldKeyListener = new TextFieldKeyListener();
        //authorField.addKeyListener(fieldKeyListener);
        timeField.addKeyListener(fieldKeyListener);
        endtimeField.addKeyListener(fieldKeyListener);
        tagsField.addKeyListener(fieldKeyListener);
        //typeField.addKeyListener(fieldKeyListener);

        //noteText.addKeyListener(new TextAreaKeyListener());


        //Adding the elements to the dialog
        //JScrollPane textScroll = new JScrollPane(noteText);
        //textScroll.setAlignmentX(LEFT_ALIGNMENT);
        //textScroll.setPreferredSize(new Dimension(150, 120));

        JPanel textPane = new JPanel();
        textPane.setLayout(new BoxLayout(textPane, BoxLayout.PAGE_AXIS));

        //textPane.add(AUTHOR_LABEL);
        //textPane.add(authorField);
        textPane.add(TIME_LABEL);
        textPane.add(timeField);
        textPane.add(ENDTIME_LABEL);
        textPane.add(endtimeField);
        textPane.add(TAGS_LABEL);
        textPane.add(tagsField);
        //textPane.add(TYPE_LABEL);
        //textPane.add(typeField);
        //textPane.add(Box.createRigidArea(new Dimension(0, 5)));
        //textPane.add(TEXT_LABEL);
        //textPane.add(textScroll);
        textPane.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

        saveButton.addActionListener(new SaveActionListener());
        cancelButton.addActionListener(new CancelActionListener());

        JPanel buttonPane = new JPanel();
        buttonPane.setLayout(new BoxLayout(buttonPane, BoxLayout.LINE_AXIS));
        buttonPane.setBorder(BorderFactory.createEmptyBorder(0, 10, 10, 10));
        buttonPane.add(saveButton);
        buttonPane.add(cancelButton);
        buttonPane.add(Box.createRigidArea(new Dimension(10, 0)));

        this.add(textPane, BorderLayout.CENTER);
        this.add(buttonPane, BorderLayout.PAGE_END);
    }

    public class TextFieldKeyListener
        extends KeyAdapter
    {

        @Override
        public void keyPressed(KeyEvent e)
        {
            super.keyPressed(e);

            if (e.getKeyCode() == KeyEvent.VK_ENTER)
            {
                saveButton.doClick();
            }
            else if (e.getKeyCode() == KeyEvent.VK_ESCAPE)
            {
                cancelButton.doClick();
            }
        }

    }

    public class TextAreaKeyListener
        extends KeyAdapter
    {

        @Override
        public void keyPressed(KeyEvent e)
        {
            super.keyPressed(e);

            if ((e.getKeyCode() == KeyEvent.VK_ENTER))
            {

                int shiftMask = KeyEvent.SHIFT_DOWN_MASK;

                // If SHIFT is being held down, then do a carriage return.
                if ((e.getModifiersEx() & shiftMask) == shiftMask)
                {
                    //noteText.append("\n");
                }
                else
                {
                    saveButton.doClick();
                }
            }
            else if (e.getKeyCode() == KeyEvent.VK_ESCAPE)
            {
                cancelButton.doClick();
            }
        }

    }

    /**
     * Action listener class that listens for when the save button is pressed.
     * @author elgoodm
     *
     */
    class SaveActionListener
        implements ActionListener
    {

        @Override
        public void actionPerformed(ActionEvent e)
        {
            RBBEventSpecDialog.this.dispose(); //get rid of the dialog

            //Create an annotation based on the fields
            //String text = noteText.getText();
            //String author = authorField.getText();
            //String type = typeField.getText();
            //long duration = DefaultAnnotation.timeStringToMilliseconds(
            //        durationField.getText().trim());
            //long time = DefaultAnnotation.timeStringToMilliseconds(
            //        timeField.getText().trim());

            //Object id = parent.getAnnotation().getId();
            //editData = new DefaultAnnotation(id, time, duration, type, text,
            //                                 author);
            //parent.getTimelineView().requestChangeAnnotation(editData);

            Long startTime = new Long(timeField.getText());
            Long endTime = new Long(endtimeField.getText());
            //Long endTime = startTime + duration;
            try
            {
                System.err.format("rbb create event with %f/%f/%s/%n", startTime.doubleValue()
                    / 1000.0, endTime.doubleValue() / 1000.0,
                    tagsField.getText());
                new Event(rbb.db(), startTime.doubleValue() / 1000.0, endTime.doubleValue()
                    / 1000.0, new Tagset(tagsField.getText()));
            }
            catch (Exception exception)
            {
                System.err.println(exception.getMessage());
            }
            System.err.println("SAVE ME");
        }

    }

    /**
     * Action listener class that listens for when the cancel button is pressed.
     * @author elgoodm
     *
     */
    class CancelActionListener
        implements ActionListener
    {

        @Override
        public void actionPerformed(ActionEvent e)
        {
            RBBEventSpecDialog.this.dispose(); //get rid of the dialog
        }

    }

}
