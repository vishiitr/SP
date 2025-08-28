using System;
using System.Threading;
using CADIS.Common;
namespace CADIS.Service.Common

{
    /// <summary> 	
    /// Heartbeat class for periodic server processes.   	
    /// </summary> 	
    /// <remarks> 	
    /// Save excessing calls to server for current time (with periodic resyncs)	 	
    /// Facilitates testing of time period service code 	
    /// Abstract repeated boilerplate timer locking code away from client code 	
    /// </remarks> 
    public class ServerTimeHeartbeat : IServerTimeHeartbeat
    {
        private readonly IServerInfo mServerInfo;
        private readonly int mIntervalSecs;
        private readonly int mTicksBetweenResync;
        private readonly bool mTickOnStart;
        private readonly object mTimerThreadLock = new Object();
        private readonly System.Timers.Timer mPollIntervalTimer;
        private DateTime mServerTime;
        private int mTicksUntilResync;
        private bool mRunning;
        public event TickEventHandler TickEvent;
        public bool Running
        {
            get
            {
                return mRunning;
            }
        }
        public ServerTimeHeartbeat(IServerInfo serverInfo, bool tickOnStart, int intervalSecs, int serverTimeResyncMins = 5)
        {
            mRunning = false;
            mServerInfo = serverInfo;
            mTickOnStart = tickOnStart;
            mIntervalSecs = intervalSecs;
            mTicksBetweenResync = CalcTicksBetweenResync(serverTimeResyncMins);
            mPollIntervalTimer = CreateTimer();
        }
        int CalcTicksBetweenResync(int serverTimeResyncMins)
        {
            float ticksPerResync = (serverTimeResyncMins * 60.0f) / (float)mIntervalSecs;
            return (ticksPerResync < 1) ? 1 : (int)Math.Ceiling(ticksPerResync);
        }
        private System.Timers.Timer CreateTimer()
        {
            var timer = new System.Timers.Timer();
            timer.Elapsed += new System.Timers.ElapsedEventHandler(mEventPollTimer_Elapsed);
            timer.Enabled = false;
            timer.Interval = mIntervalSecs * 1000;
            timer.AutoReset = true;
            return timer;
        }
        void mEventPollTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            if (Monitor.TryEnter(mTimerThreadLock))
            {
                try
                {
                    UpdateTime();
                    RaiseTickEvent();
                }
                finally
                {
                    Monitor.Exit(mTimerThreadLock);
                }
            }
        }
        private void RaiseTickEvent()
        {
            if (TickEvent != null)
            {
                foreach (Delegate receiver in TickEvent.GetInvocationList())
                {
                    ((TickEventHandler)receiver).BeginInvoke(mServerTime, null, null);
                }
            }
        }
        /// <summary> 	
        /// Time is periodically resynced from server, or incremented by tick period 	
        /// to avoid unecessary repeated calls to server for a time a known interval ago 	
        /// </summary> 	private 
        void UpdateTime()
        {
            if (--mTicksUntilResync == 0)
            {
                RefreshTimeFromServer();
            }
            else
            {
                mServerTime = mServerTime.AddSeconds(mIntervalSecs);
            }
        }
        /// <summary> 	
        /// Resync clock, and restart tick countdown until next sync 	
        /// </summary> 
        private void RefreshTimeFromServer()
        {
            mTicksUntilResync = mTicksBetweenResync;
            mServerTime = TruncateMilliseconds(SqlHelper.Service.Process.GetServerTime(mServerInfo.NewCallInfo()));
        }
        /// <summary> 	
        /// Lowest resolution is 1 second 	
        /// </summary> 	
        private static DateTime TruncateMilliseconds(DateTime dateTime)
        {
            return dateTime.AddTicks(-(dateTime.Ticks % TimeSpan.TicksPerSecond));
        }
        public void Start()
        {
            lock (mTimerThreadLock)
            {
                mPollIntervalTimer.Stop();
                RefreshTimeFromServer();
                if (mTickOnStart) RaiseTickEvent();
                mPollIntervalTimer.Start();
                mRunning = true;
            }
        }
        public void Stop()
        {
            lock (mTimerThreadLock)
            {
                mPollIntervalTimer.Stop();
                mRunning = false;
            }
        }
        ~ServerTimeHeartbeat()
        {
            try
            {
                this.Stop();
                TickEvent = null;
            }
            catch { }
        }
    }
}
