package gov.sandia.rbb.ui;

import gov.sandia.rbb.RBB;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * Control mplayer via ReplayControl.
 *
 * A big limitation of this class is that mplayer does not act as a controller -
 * if the user hits the keys to move in time, the ReplayControl will NOT be notified.
 *
 * @author rgabbot
 */
public class MPlayerReplayClient implements RBBReplayControl.Listener {
    Process process;
    PrintStream writeToMplayer;
    BufferedReader readFromMplayer;
    BufferedReader readErrorsFromMplayer;

    Long duration;

    // we would like mplayer to pause at end of file instead of exiting.
    // However, it doesn't have that option.
    // Instead, whenever we start it playing, we schedule a timer to go off
    // a little before end of file and pause it.
    // Yes, it's an ugly hack.  But I don't have a better idea.
    Timer endOfFileTimer;
    TimerTask endOfFileTimerTask; // keep a reference so it can be canceled if we don't play to the end.
    // in trying to go past the end of file, we cannot be precise because
    // 1) seek is not precise - e.g. video streams can only seek to key frames
    // 2) the timer that pauses at end of file won't awake instantly
    // 3) can't assume mplayer plays at precise rate.
    // So to compensate for all that, pick an arbitrary safety margin in milliseconds.
    final long endOfFileSafetyMargin = 1000;

    private Integer exitStatus;

    public MPlayerReplayClient(RBB coordinationRBB, final String media_file) throws Exception
    {
        this.endOfFileTimer = new Timer();
        
        System.err.println("MplayerReplayClient: opening " + media_file);

        // The -loop option is used so mplayer will stay alive even if mplayer ends up playing or seeking past the end of file.
        // This is supposed to not happen, thanks to the endOfFileTimer.
        // But, for example, if the user controls mplayer directly using its keyboard input, it could end up playing to the end of file and exiting.
        // -loop is used rather than -idle, which  causes mplayer to not exit when the file ends, but the window still disappears and the file is closed.
        // Instead this uses -loop 0, i.e. loop forever.  This has a few issues:
        // 1) after playing once, it will start again, showing the start when it should be at or past the end.
        // 2) looping interferes with slave mode, so it becomes unresponsive for the next few time seeks.
        // 3) loop destroys the playback window, so its size and placement are lost.

        // -use-filename-title sets the title of the window to the name of the file.  It works on OSX/Linux but not Windows.

        // -vo gl is being used because under windows running in VMWare, the default -vo directx just shows a green screen about 20% of the time.
        // This occurs even if mplayer is run directly from the command-line, not through this calss.
        // If it is determined that never happens under a real windows install (as opposed to in VMWare), it could be removed.
        // The comma after "gl,"  allows it to fall back on some other driver if opengl is not available, for example in the RHEL VM
        
        this.process = Runtime.getRuntime().exec(new String[] {
            "mplayer", "-use-filename-title", 
                "-vo", "gl,", 
                "-loop", "0", "-quiet", "-slave", "-osdlevel", "0", media_file
        });

        // NOTE:
        // writeToMplayer is used to write commands to mplayer.
        // The commands must end in '\n', even under Windows (not '\r\n')
        // So do NOT use writeToMplayer.println("blah"); instead use
        // writeToMplayer.print("blah\n");
        this.writeToMplayer = new java.io.PrintStream(this.process.getOutputStream());

        this.readFromMplayer = new java.io.BufferedReader(new java.io.InputStreamReader(this.process.getInputStream()));

        this.readErrorsFromMplayer = new java.io.BufferedReader(new java.io.InputStreamReader(this.process.getErrorStream()));


        // initialize() takes a moment as mplayer spins up, so kick off a thread for it.
        Thread initThread = new Thread("MPlayerReplayClient initializer") {
            @Override
            public void run() {
                initialize();
                //// for some reason this seems to make the initThread keep ownership of this object,
                // even after initialize() (which is synchronized) exits.
//                System.err.println("MPlayerReplayClient done initializing.");
//
//                // after initialze nothing is every read from mplayer stdout.
//                try {
//                    String s;
//                    while ((s = readFromMplayer.readLine())!=null) {
//                        System.err.println(s);
//                    }
//                } catch (IOException ex) {
//                    System.err.println("Error reading stdout from MPlayer " + media_file);
//                }
            }
        };
        initThread.start();

        // start a thread to get MPlayer's stderr and print to System.err
        new Thread("MPlayerReplayClient readErrorsFromMplayer") {
            @Override
            public void run() {
                try {
                    String s;
                    while ((s = readErrorsFromMplayer.readLine())!=null) {
                        System.err.println(s);
                    }
                } catch (IOException ex) {
                    System.err.println("Error reading stderr from MPlayer " + media_file);
                }
                // System.err.println("MPlayerReplayClient done reading errors from stderr.");
            }
        }.start();


        final RBBReplayControl replayControl = new RBBReplayControl(coordinationRBB, this, null);


        // start a thread to reap the exit status of mplayer.
        new Thread("MplayerReplayClient cleanup") {
            @Override
            public void run() {
                try {
                    exitStatus = process.waitFor();
                    System.err.println("MPlayer exited with status " + exitStatus);
                } catch (InterruptedException ex) {
                    System.err.println("MPlayer noticeExit thread interrupted from sleep.  Setting exitStatus to -1.");
                    exitStatus = -1;
                } finally {
                    endOfFileTimer.cancel(); // if this is not done the time thread will keep the process from exiting.
                    endOfFileTimer = null;
                    replayControl.disconnect(); // if this is not done then replayControl (which is an eventListener) will keep the process from exiting.
                }
            }
        }.start();

    }

    /**
     * Returns the exit status of mplayer, or null
     * if it has not yet exited.
     *
     */
    public Integer getExitStatus() {
        return exitStatus;
    }

    public Integer waitForMPlayerToExit() {
        try {
            return process.waitFor();
        } catch (InterruptedException ex) {
            return null;
        }
    }

    /**
     * Get the duration, in milliseconds, of the currently playing file.
     * This is synchronized so it won't return garbage before initialize()
     * (which is run in a separate thread) is done.
     */
    public synchronized Long getDuration() throws IOException {
        System.err.println("MPlayerReplayClient returning duration of " + this.duration);
        return this.duration;
    }

    @Override public synchronized void replayControl(long simTime, double playRate) {
        discardJunk();

        String cmd = "";

        if(endOfFileTimerTask != null) {
            endOfFileTimerTask.cancel();
            endOfFileTimerTask=null;
        }

        // try to avoid seeking past end of file.
        if (this.duration != null && simTime + endOfFileSafetyMargin > this.duration) {
            pauseAtEnd();
            return;
        } 
        
        if (playRate > 0) {
            endOfFileTimerTask = new TimerTask() {
                public void run() {
                    // System.err.println("auto pause at end of file.");
                    pauseAtEnd();
                }
            };
            this.endOfFileTimer.schedule(endOfFileTimerTask, this.duration - simTime - this.endOfFileSafetyMargin);
        } else {
            cmd += "pausing ";
        }
        cmd += "seek " + simTime / 1000.0 + " 2";

        this.writeToMplayer.print(cmd+"\n");
        this.writeToMplayer.flush(); // yes, this is necessary.
    }

    /**
     * Stop mplayer at any time.
     * This doesn't need to be called for cleanup if mplayer has already exited.
     */
    public void destroy() {
        if (process != null) {
            process.destroy();
            process = null;
        }
    }

    private synchronized void discardJunk() {
        try {
            while (this.readFromMplayer.ready()) {
                String s = this.readFromMplayer.readLine();
                // System.err.println("MPlayerReplayClient discarding junk: " + s);
            }
        } catch (IOException ex) {
            Logger.getLogger(MPlayerReplayClient.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private synchronized void pauseAtEnd() {
        String cmd = "pausing seek " + (this.duration - this.endOfFileSafetyMargin) / 1000.0 + " 2";
        this.writeToMplayer.print(cmd+"\n");
        this.writeToMplayer.flush(); // yes, this is necessary.
    }

    private synchronized void initialize() {
        // the purpose of the following block of code is to wait until mplayer initializes itself
        // and becomes responsive to slave commands.  This is tricky because:
        // 1) mplayer cannot start paused, which is what we want.
        // 2) mplayer is unresponsive to slave commands for an indeterminate amount of time at startup.
        // 3) if mplayer is paused, even asking it whether it is paused (pausing get_property pause) cause it to un-pause and report it isn't paused.
        // 4) if mplayer is paused and told not to un-pause, it is unresponsive to a query on whether it is paused. (pausing_keep_force get_property pause)
        // So the strategy here is to just keep asking until we get an initial report that it is not paused.  After that, it should be responsive to a command to return to time 0 and pause.
        try {
            for (boolean responsive = false; !responsive; Thread.sleep(50)) {
                // the purpse of this nested loop structure is:
                // 1) don't sleep so long as readFromMplayer is ready.
                // 2) yet don't exit until it isn't ready even after sleeping.
                while (!responsive && this.readFromMplayer.ready()) {
                    String s = this.readFromMplayer.readLine();
                    if (s == null) {
                        throw new IOException("MPlayerReplayClient: error starting up.");
                    } else if (s.equals("ANS_pause=no")) {
                        // System.err.println("MPlayerReplayClient: started playback.");
                        responsive = true;
                    } else {
                        // System.err.println("MPlayerReplayClient discarding initial junk: " + s);
                    }
                }

                this.writeToMplayer.print("get_property pause\n");
                this.writeToMplayer.flush();
                //              System.err.println("MPlayerReplayClient sleeping.");
            }

            // now, it's possible we issued "get_property pause" more than once after it became responsive but before we got the answer,
            // but we cannot know how many are in that backlog.
            // So, assume the backlog will be cleared with pauses no longer than 50ms
            for (Thread.sleep(50); this.readFromMplayer.ready(); Thread.sleep(50)) {
                while (this.readFromMplayer.ready()) {
                    String s = this.readFromMplayer.readLine();
                    // System.err.println("MPlayerReplayClient discarding more initial junk: " + s);
                }
                // System.err.println("MPlayerReplayClient sleeping more.");
            }


            // now that mplayer is responsive, we can issue commands...

            // get and store the duration.
            this.writeToMplayer.print("get_time_length\n");
            this.writeToMplayer.flush();
            final String s = this.readFromMplayer.readLine();
            final String[] a = s.split("=", 2);
            if (a.length == 2 && a[0].equals("ANS_LENGTH")) {
                this.duration = (long) (1000.0 * Double.parseDouble(a[1]));
            }
            // System.err.println("duration of " + media_file + " is " + this.duration);

            // pause it back at t=0
            this.writeToMplayer.print("pausing seek 0 2\n");
            this.writeToMplayer.flush();

        } catch (InterruptedException ex) {
            Logger.getLogger(MPlayerReplayClient.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(MPlayerReplayClient.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
