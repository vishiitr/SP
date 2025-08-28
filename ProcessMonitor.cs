using System;
using System.Collections.Generic;
using System.Text;
namespace CADIS.CPU
{
    /// <summary> 	
    /// Monitors the specified process ID and notifies the user when the process is no longer running. 	
    /// More reliable and less resource intensive than monitoring the process using events 	
    /// </summary> 	
    public class ProcessMonitor
    {
        public event EventHandler ProcessExited;
        Int32 mPid;
        System.Timers.Timer mTimer;
        Object mLock = new Object();
        public ProcessMonitor(Int32 pid)
        {
            mPid = pid;
            mTimer = new System.Timers.Timer(5000);
            mTimer.AutoReset = false;
            mTimer.Elapsed += new System.Timers.ElapsedEventHandler(Timer_Elapsed);
            mTimer.Start();
        }
        public Int32 ProcessID
        {
            get
            {
                lock (mLock)
                {
                    return mPid;
                }
            }
        }
        public Boolean IsRunning
        {
            get
            {
                try
                {
                    lock (mLock)
                    {
                        System.Diagnostics.Process p = System.Diagnostics.Process.GetProcessById(mPid);
                        return true;
                    }
                }
                catch (ArgumentException)
                {
                    return false;
                }
            }
        }
        public void KillProcess()
        {
            try
            {
                lock (mLock)
                {
                    System.Diagnostics.Process p = System.Diagnostics.Process.GetProcessById(mPid);
                    p.Kill();
                }
            }
            catch (ArgumentException)
            {
                // Process not found, assumed already ended 	
            }
        }
        void Timer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            if (this.IsRunning)
            {
                // Restart the timer 	
                mTimer.Start();
            }
            else
            {
                // Process not found 	
                OnProcessExited();
            }
        }
        void OnProcessExited()
        {
            if (ProcessExited != null) ProcessExited(this, EventArgs.Empty);
        }
    }
}
