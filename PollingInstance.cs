using System;
using System.Diagnostics;
using System.Threading;
using CADIS.Common;
using CADIS.Service.Common;
namespace CADIS.Service
{
    /// <summary> 	
    /// Adapter class to host polling process instance in a service instance 	
    /// </summary> 	
    public class PollingInstance : IProcessInstance
    {
        private readonly object mHeartbeatThreadLock = new Object();
        private readonly IServerTimeHeartbeat mServerTimeHeartbeat;
        private readonly IProcessInstancePolling mProcessInstancePolling;
        private readonly IProcessComponent mProcess;
        private readonly IServiceLogger mServiceLogger;
        public PollingInstance(IProcessComponent processComponent, IProcessInstancePolling processInstancePolling, IServiceLogger serviceLogger, IServerInfo serverInfo) : this(processComponent, processInstancePolling, serviceLogger, new ServerTimeHeartbeat(serverInfo, false, processComponent.PollingInterval)) { }
        // Allow DI of heartbeat for testing 	
        public PollingInstance(IProcessComponent processComponent, IProcessInstancePolling processInstancePolling, IServiceLogger serviceLogger, IServerTimeHeartbeat serverTimeHeartbeat)
        {
            mProcessInstancePolling = processInstancePolling;
            mProcess = processComponent;
            mServerTimeHeartbeat = serverTimeHeartbeat;
            mServiceLogger = serviceLogger;
        }
        private void ServerTimeHeartbeat_Tick(DateTime serverTime)
        {
            // If tick frequency set such that previous tick still running, just 	
            // fall though until next call to avoid unnecessary deadlocking 	
            if (Monitor.TryEnter(mHeartbeatThreadLock))
            {
                try
                {
                    LogMessage($ "Poll instance heartbeat start. ", $ "Server Time : {serverTime}", ServiceLogLevel.Debug);
                    var stopwatch = Stopwatch.StartNew();
                    PrePoll(serverTime);
                    stopwatch.Stop();
                    LogMessage($ "Poll instance heartbeat complete. ", $ "Duration : {stopwatch.Elapsed.TotalSeconds} s", ServiceLogLevel.Debug);
                }
                catch (Exception ex)
                {
                    // No exceptions should ever be thrown out of this method as they will crash the service! 	
                    LogException(ex);
                }
                finally
                {
                    Monitor.Exit(mHeartbeatThreadLock);
                }
            }
            else
            {
                LogMessage("Service heartbeat skipped. ", "Previous processing cycle still executing", ServiceLogLevel.Debug);
            }
        }
        /// <summary> 	
        /// Fired when the event time elapses.  Runs on a background thread 	
        /// This is used to check whether the process definition has changed and whether it needs to be reloaded 	
        /// </summary> 
        private void PrePoll(DateTime serverTime)
        {
            // Quit if the process is not active (i.e. either disabled or not within watch period)
            if (!mProcess.IsActive(serverTime)) return;
            // Poll the watch instance 	
            LogMessage("Polling the process.", "", ServiceLogLevel.Debug);
            mProcessInstancePolling.Poll();
        }
        public System.Runtime.Serialization.ISerializable GetState()
        {
            LogMessage("Getting current process state.", "", ServiceLogLevel.Debug);
            return mProcessInstancePolling.GetState();
        }
        public void RestoreState(System.Runtime.Serialization.ISerializable state)
        {
            LogMessage("Restoring process state.", "", ServiceLogLevel.Debug);
            mProcessInstancePolling.RestoreState(state);
        }
        public void StartProcess()
        {
            // In future the polling instance will host an IMessageTarget and there will be a : 	
            //mMessageTarget.Initialise();
            // call to it here that will perform the failover prior to polling starting 	
            // (via the ProcessRunFailover domain service who's logic should be push/poll independent) 	
            LogMessage("Starting polling process.", "", ServiceLogLevel.Debug);
            mProcessInstancePolling.Initialise();
            StartHeartbeat();
        }
        public void StopProcess()
        {
            LogMessage("Stopping polling process.", "", ServiceLogLevel.Debug);
            StopHeartbeat();
            mProcessInstancePolling.Terminate();
        }
        private void StartHeartbeat()
        {
            LogMessage("Starting poll instance heartbeat.", "", ServiceLogLevel.Debug);
            if (mServerTimeHeartbeat.Running) StopHeartbeat();
            mServerTimeHeartbeat.TickEvent += new TickEventHandler(ServerTimeHeartbeat_Tick);
            mServerTimeHeartbeat.Start();
        }
        private void StopHeartbeat()
        {
            LogMessage("Stopping poll instance heartbeat.", "", ServiceLogLevel.Debug);
            mServerTimeHeartbeat.Stop();
            mServerTimeHeartbeat.TickEvent -= new TickEventHandler(ServerTimeHeartbeat_Tick);
        }
        private void LogMessage(string summary, string detail, ServiceLogLevel logLevel)
        {
            mServiceLogger.LogMessage(mProcess.ComponentKey, mProcess.Guid, mProcess.Name, summary, detail, logLevel);
        }
        public void LogException(Exception ex)
        {
            mServiceLogger.LogException(mProcess.ComponentKey, mProcess.Guid, mProcess.Name, ex);
        }
    }
}
