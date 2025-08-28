using System;
using System.Collections.Generic;
using System.Text;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using CADIS.DataFlow.Runtime.Core;
using CADIS.DataFlow.Runtime.Data;
using System.Globalization;
using CADIS.Common;
using System.Dynamic;
namespace CADIS.DataFlow.Runtime.Distribution
{
    /// <summary> 	
    /// The head of a number of parallel processing chains. 	
    /// This class manages the distribution of a single set of data to a number of threads at high speed.  It uses 	
    /// buffers to load up each thread with work to do.  It is only efficient with multiple threads. 	
    /// 	
    /// It tries to be fair by ensuring each thread gets a similar amount of data to start with, and then responds 	
    /// to each thread's request for more data as required. 	
    /// Critically, each thread waits to be given more data.  Therefore this class is not swamped with threads 	
    /// requesting data directly, they are queued and served more data when the Distributor is ready. 	
    /// 	
    /// NOTE: This class uses advanced multi-threading techniques and should not be modified except with EXTREME care! 	
    /// </summary> 	
    public class DataDistributor : HeadBase, IDistributor
    {
        const double WAIT_WARNING_THRESHOLD = 35.0;
        Stopwatch mElapsedTimer;
        Stopwatch mWaitingTimer;
        // An auto-reset because we use it multiple times and just want a pulse notification 	
        AutoResetEvent mWaitForEmptyThreads;
        // A manual-reset because this is only signalled once 	
        ManualResetEvent mWaitForAllThreadsToComplete;
        const String CONTEXT = "Distributor";
        List<DistributedThread> mAllThreads;
        // I choose to use a queue so that each thread gets a fair share of the workload.  The downside is that 	
        // if too many threads are created, each doesn't do a lot of work. 	
        ConcurrentQueue<DistributedThread> mEmptyThreads;
        Int64 mThreadsRunning;
        Int64 mFailedThreads;
        IBufferReader mReader;
        Boolean mRunOnce;
        public DataDistributor(IBufferReader reader, ThreadLog log, CultureInfo cultureToRunAs, Int32 runID, Int32 parentRunId, Int32 topLevelRunId, ICallInfo ci) : base(log, cultureToRunAs, runID, parentRunId, topLevelRunId, ci)
        {
            mReader = reader;
            mAllThreads = new List<DistributedThread>();
            mEmptyThreads = new ConcurrentQueue<DistributedThread>();
            // Set to true initially as we don't want to wait the first time in (all threads are empty) 	mWaitForEmptyThreads = new AutoResetEvent(true); 	mWaitForAllThreadsToComplete = new ManualResetEvent(false); 	mElapsedTimer = new Stopwatch(); 	mWaitingTimer = new Stopwatch(); 	
            // The initial state of signals above is vital.  Therefore we force the case that we can only run this process 	
            // once without throwing an exception.  	
            // This is a preemptive safety measure to avoid some nasty thread timing issues which were seen 	
            // when debugging this code. 	
            mRunOnce = false;
        }
        public void AddThread(Int32 id, Int32 bufferSize, IDistributedTarget target)
        {
            // Wrap the distribution target in our high-performance buffering class 	
            DistributedThread thread = new DistributedThread(this.Log, this, id, bufferSize, target, mCultureToRunAs);
            mAllThreads.Add(thread);
            // Initially, all threads are empty 	
            mEmptyThreads.Enqueue(thread);
        }
        protected override void RunProcess()
        {
            // Initialisation of the signals CAN ONLY be done in the constructor, before the process starts running, to 	
            // avoid race conditions.  Use a flag just to make sure that a Dev doesn't change this in the future by accident 	
            if (mRunOnce) throw new ApplicationException("The distributor can only be run once.  Recreate the object to ensure it is initialised correctly, don't run it twice");
            mRunOnce = true;
            // Set the total number of threads so we can monitor when all have completed 	
            Interlocked.Exchange(ref mThreadsRunning, mAllThreads.Count);
            Interlocked.Exchange(ref mFailedThreads, 0);
            // Start a thread for each of the distributed workers.  They will enter their wait state 	
            // until we feed them their first set of data 	
            foreach (DistributedThread thread in mAllThreads)
            {
                ThreadPool.QueueUserWorkItem(thread.Run);
            }
            // Loop forever servicing empty threads, until there's no more data and all threads have completed 	
            Boolean endOfData = false;
            mElapsedTimer.Start();
            while (true)
            {
                this.Log.LogMessage(ThreadLog.LogLevel.DEBUG, CONTEXT, "Waiting for empty or completed threads");
                // Wait until either all threads have completed or new empty threads have been added to the queue 	
                mWaitingTimer.Start();
                Int32 waitFlagged = WaitHandle.WaitAny(new WaitHandle[] {
                              mWaitForAllThreadsToComplete,
                              mWaitForEmptyThreads
                            });

                mWaitingTimer.Stop();
                // If the WaitForAllThreadsComplete signal was set, quit the loop 	
                if (waitFlagged == 0)
                {
                    this.Log.LogMessage(ThreadLog.LogLevel.DEBUG, CONTEXT, "Received \"all threads completed\" signal");
                    mElapsedTimer.Stop();
                    int waitpercent = mElapsedTimer.ElapsedMilliseconds == 0 ? 0 : (int)((100.0d * mWaitingTimer.ElapsedMilliseconds) / mElapsedTimer.ElapsedMilliseconds);
                    this.Log.LogMessage(ThreadLog.LogLevel.DEBUG, CONTEXT, "Distributor was waiting for threads to complete {0}% of the time.", waitpercent);
                    this.Log.LogMessage(ThreadLog.LogLevel.INFO, CONTEXT, "Distributor was waiting for threads to complete {0}% of the time.", waitpercent);
                    break;
                }
                this.Log.LogMessage(ThreadLog.LogLevel.DEBUG, CONTEXT, "Received empty threads signal.  Threads in empty buffer queue: {0}", mEmptyThreads.Count);
                // Load all the empty threads with data whilst there is data available
                while (!mEmptyThreads.IsEmpty)
                {
                    // Remove the first item from the empty threads list and refill it
                    DistributedThread thread;
                    mEmptyThreads.TryDequeue(out thread);
                    // Check whether any other threads have failed.  If so, stop good threads once their buffers are empty 	
                    if (Interlocked.Read(ref mFailedThreads) > 0)
                    {
                        this.Log.LogMessage(ThreadLog.LogLevel.DEBUG, CONTEXT, "Refill of thread {0} aborted because another thread has failed", thread.ID);
                        // Allow to fall into the end of data processing, which stops the threads naturally 	
                        endOfData = true;
                    }
                    if (endOfData)
                    {
                        this.Log.LogMessage(ThreadLog.LogLevel.DEBUG, CONTEXT, "Signalling thread {0} \"no more data\"", thread.ID);
                        // We are at the end of the data source, so we can't refill the thread.  Indicate to it that 	
                        // it should now stop when it's ready 	
                        thread.SignalNoMoreData();
                    }
                    else
                    {
                        // Read a set of lines from the data source 	
                        Int32 rowCount = thread.RefillQueue(mReader);
                        // Set the flag if we've hit the end of the data 	
                        endOfData = mReader.AtEnd;
                        this.Log.LogMessage(ThreadLog.LogLevel.DEBUG, CONTEXT, "Refill of thread {0} complete with {1} rows.  At end of data: {2}", thread.ID, rowCount, endOfData);
                    }
                }
            }
            this.Log.LogMessage(ThreadLog.LogLevel.DEBUG, CONTEXT, "Clearing up");
            // Capture any exceptions thrown by the worker threads 	
            foreach (DistributedThread thread in mAllThreads)
            {
                if (thread.LastException != null) this.Exceptions.Add(thread.LastException);
            }
        }
        void IDistributor.NotifyQueueEmpty(DistributedThread thread)
        {
            // NOTE: Runs on the DistributedThread background thread! 	
            this.Log.LogMessage(ThreadLog.LogLevel.DEBUG, CONTEXT, "Thread {0} notified its buffer is empty", thread.ID);
            mEmptyThreads.Enqueue(thread);
            // Tell the main loop that there's work to be done
            mWaitForEmptyThreads.Set();
        }
        void IDistributor.NotifyComplete(DistributedThread thread, Boolean failed)
        {
            // NOTE: Runs on the DistributedThread background thread! 	
            if (failed)
            {
                this.Log.LogMessage(ThreadLog.LogLevel.DEBUG, CONTEXT, "Thread {0} notified it failed", thread.ID);
                // Indicate we should stop at the next appropriate opportunity 	
                Interlocked.Increment(ref mFailedThreads);
            }
            else
            {
                this.Log.LogMessage(ThreadLog.LogLevel.DEBUG, CONTEXT, "Thread {0} notified it is complete", thread.ID);
            }
            // Decrement the counter in a thread-safe way
            Int64 remaining = Interlocked.Decrement(ref mThreadsRunning);
            if (remaining <= 0)
            {
                // Signal to the main loop that we're done 	
                this.Log.LogMessage(ThreadLog.LogLevel.DEBUG, CONTEXT, "All threads are complete");
                mWaitForAllThreadsToComplete.Set();
            }
        }
        protected override void Close(ConcurrentDictionary<int, RunTimer> metrics)
        {
            try
            {
                foreach (DistributedThread thread in mAllThreads)
                {
                    thread.Close(metrics);
                }
            }
            finally
            {
                mReader.Close(metrics);
            }
        }
        public override long TotalTransactions
        {
            get
            {
                return mReader.TotalTransactions;
            }
        }
    }
}
