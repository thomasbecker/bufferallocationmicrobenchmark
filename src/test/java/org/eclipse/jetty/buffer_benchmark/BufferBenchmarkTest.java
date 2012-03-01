// ========================================================================
// Copyright (c) 2009-2009 Mort Bay Consulting Pty. Ltd.
// ------------------------------------------------------------------------
// All rights reserved. This program and the accompanying materials
// are made available under the terms of the Eclipse Public License v1.0
// and Apache License v2.0 which accompanies this distribution.
// The Eclipse Public License is available at 
// http://www.eclipse.org/legal/epl-v10.html
// The Apache License v2.0 is available at
// http://www.opensource.org/licenses/apache2.0.php
// You may elect to redistribute this code under either of these licenses. 
// ========================================================================

package org.eclipse.jetty.buffer_benchmark;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeDataSupport;

import org.eclipse.jetty.io.Buffer;
import org.eclipse.jetty.io.Buffers;
import org.eclipse.jetty.io.PooledBuffers;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;

/* ------------------------------------------------------------ */
/**
 */
public class BufferBenchmarkTest
{
    private static final int ITERATIONS = 10;
    private static final int NUMBER_ALLOCATIONS = 1000000;
    private static final int THREADS = 64;
    private ThreadPoolExecutor threadPool;
    private GarbageCollectorMXBean youngCollector;
    private GarbageCollectorMXBean oldCollector;
    private GarbageCollectionListener garbageCollectionListener = new GarbageCollectionListener();
    private long startTime = 0;
    private long youngCollectionTimeBefore = 0;
    private long oldCollectionTimeBefore = 0;
    private long survivorUsed = 0;
    private long edenUsed = 0;
    private long oldGenUsed = 0;
    private long codeCacheUsed = 0;
    private long permGenSpaceUsed = 0;
    private long totalMemUsed = 0;

    /* ------------------------------------------------------------ */
    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        resetThreadPoolExecutor();
        List<GarbageCollectorMXBean> garbageCollectors = ManagementFactory.getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean garbageCollector : garbageCollectors)
        {
            if ("PS Scavenge".equals(garbageCollector.getName()) || "ParNew".equals(garbageCollector.getName())
                    || "G1 Young Generation".equals(garbageCollector.getName()))
            {
                youngCollector = garbageCollector;
                NotificationEmitter notificationEmitter = (NotificationEmitter)youngCollector;
                notificationEmitter.addNotificationListener(garbageCollectionListener,null,null);
            }
            else if ("PS MarkSweep".equals(garbageCollector.getName()) || "ConcurrentMarkSweep".equals(garbageCollector.getName())
                    || "G1 Old Generation".equals(garbageCollector.getName()))
            {
                oldCollector = garbageCollector;
                NotificationEmitter notificationEmitter = (NotificationEmitter)oldCollector;
                notificationEmitter.addNotificationListener(garbageCollectionListener,null,null);
            }
        }
    }

    private class GarbageCollectionListener implements NotificationListener
    {

        @Override
        public void handleNotification(Notification notification, Object handback)
        {
            CompositeDataSupport userData = (CompositeDataSupport)notification.getUserData();
            GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from(userData);
            GcInfo gcInfo = info.getGcInfo();
            Map<String, MemoryUsage> memoryUsageBeforeGC = gcInfo.getMemoryUsageBeforeGc();
            Map<String, MemoryUsage> memoryUsageAfterGC = gcInfo.getMemoryUsageAfterGc();
            for (String key : memoryUsageBeforeGC.keySet())
            {
                long memoryCollected = calculateGarbageCleaned(memoryUsageBeforeGC,memoryUsageAfterGC,key);
                switch (key)
                {
                    case "PS Survivor Space":
                        survivorUsed = survivorUsed + memoryCollected;
                        break;
                    case "PS Eden Space":
                        edenUsed = edenUsed + memoryCollected;
                        totalMemUsed = totalMemUsed + memoryCollected;
                        break;
                    case "PS Old Gen":
                        oldGenUsed = oldGenUsed + memoryCollected;
                        totalMemUsed = totalMemUsed + memoryCollected;
                        break;
                    case "Code Cache":
                        codeCacheUsed = codeCacheUsed + memoryCollected;
                        break;
                    case "PS Perm Gen":
                        permGenSpaceUsed = permGenSpaceUsed + memoryCollected;
                        break;

                    default:
                        break;
                }
            }
        }

        private long calculateGarbageCleaned(Map<String, MemoryUsage> memoryUsageBefore, Map<String, MemoryUsage> memoryUsageAfter, String key)
        {
            return memoryUsageBefore.get(key).getUsed() - memoryUsageAfter.get(key).getUsed();
        }

    }

    private void resetThreadPoolExecutor()
    {
        threadPool = new ThreadPoolExecutorWithExceptionLogging(THREADS,THREADS,1,TimeUnit.MINUTES,new LinkedBlockingQueue<Runnable>(),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @After
    public void shutdown()
    {
        threadPool.shutdown();
    }

    /**
     * Blocks to make sure that we don't use any oldgen memory space
     **/
    private class ThreadPoolExecutorWithExceptionLogging extends ThreadPoolExecutor
    {
        // Semaphore semaphore = new Semaphore(1024);

        public ThreadPoolExecutorWithExceptionLogging(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                BlockingQueue<Runnable> workQueue, RejectedExecutionHandler callerRunsPolicy)
        {
            super(corePoolSize,maximumPoolSize,keepAliveTime,unit,workQueue,callerRunsPolicy);
        }

        // @Override
        // public void execute(Runnable command)
        // {
        // try
        // {
        // semaphore.acquire();
        // super.execute(command);
        // }
        // catch (InterruptedException e)
        // {
        // e.printStackTrace();
        // semaphore.release();
        // }
        // }

        @Override
        protected void afterExecute(Runnable r, Throwable t)
        {
            // if (this.getCompletedTaskCount() % 100000 == 0)
            // {
            // System.out.println("act:" + this.getActiveCount() + " largest:" + this.getLargestPoolSize() + " compl:" + this.getCompletedTaskCount());
            // }
            super.afterExecute(r,t);
            // semaphore.release();
            if (t == null && r instanceof Future<?>)
            {
                try
                {
                    Future<?> future = (Future<?>)r;
                    if (future.isDone())
                        future.get();
                }
                catch (CancellationException ce)
                {
                    t = ce;
                }
                catch (ExecutionException ee)
                {
                    t = ee.getCause();
                }
                catch (InterruptedException ie)
                {
                    Thread.currentThread().interrupt(); // ignore/reset
                }
            }
            if (t != null)
                System.out.println(t);
        }
    }

    @Test
    public void allocationTest()
    {
        allocationTestIterations(false);
    }

    @Test
    public void multiThreadedAllocationTest()
    {
        allocationTestIterations(true);
    }

    private void allocationTestIterations(boolean multiThreaded)
    {
        for (int i = 0; i <= ITERATIONS; i++)
        {
            resetMonitoring();
            boolean direct = false;
            int capacity = 8096;
            executeAllocateBufferIterations(direct,capacity,multiThreaded);
            logMonitoringResults();
        }
    }

    /**
     * Tests direct buffer allocation. Each new buffer will consume space in maxDirectMemory which is separated from the "normal" jvm memory generations.
     * Objects created in directMemory need to be cleaned up the same way as any other objects in memory. If directMemory is full a normal full GC is being
     * invoked (Bits.java line 624). This can seriously impact performance if buffers are not reused.
     */
    @Test
    @Ignore("Slow as each time maxDirectMemory is full a fullGC is being triggered. See Bits.java line 624")
    public void directAllocationTest()
    {
        directAllocationIterations(false);
    }

    /**
     * @see directAllocationTest description
     */
    @Test
    @Ignore("Slow as each time maxDirectMemory is full a fullGC is being triggered. See Bits.java line 624")
    public void multiThreadedDirectAllocationTest()
    {
        directAllocationIterations(true);
    }

    private void directAllocationIterations(boolean multiThreaded)
    {
        for (int i = 0; i <= ITERATIONS; i++)
        {
            resetMonitoring();
            boolean direct = true;
            int capacity = 8096;
            executeAllocateBufferIterations(direct,capacity,multiThreaded);
            logMonitoringResults();
        }
    }

    @Test
    public void pooledBufferTest()
    {
        pooledBufferIterations(false);
    }

    @Test
    public void multiThreadedPooledBufferTest()
    {
        pooledBufferIterations(true);
    }

    private void pooledBufferIterations(boolean multiThreaded)
    {
        for (int i = 0; i <= ITERATIONS; i++)
        {
            resetMonitoring();
            int capacity = 8096;
            PooledBuffers bufferPool = new PooledBuffers(Buffers.Type.INDIRECT,capacity,Buffers.Type.INDIRECT,capacity,Buffers.Type.INDIRECT,capacity);
            executePooledBufferIterations(capacity,bufferPool,multiThreaded);
            logMonitoringResults();
        }
    }

    @Test
    public void pooledDirectBufferTest()
    {
        pooledDirectBufferIterations(false);
    }

    @Test
    public void multiThreadedPooledDirectBufferTest()
    {
        pooledDirectBufferIterations(true);
    }

    private void pooledDirectBufferIterations(boolean multiThreaded)
    {
        for (int i = 0; i <= ITERATIONS; i++)
        {
            resetMonitoring();
            int capacity = 8096;
            PooledBuffers bufferPool = new PooledBuffers(Buffers.Type.DIRECT,capacity,Buffers.Type.DIRECT,capacity,Buffers.Type.DIRECT,capacity);
            executePooledBufferIterations(capacity,bufferPool,multiThreaded);
            logMonitoringResults();
        }
    }

    @Test
    public void allocationRandomCapacityTest()
    {
        allocationRandomCapacityIterations(false);
    }

    @Test
    public void multiThreadedAllocationRandomCapacityTest()
    {
        allocationRandomCapacityIterations(true);
    }

    private void allocationRandomCapacityIterations(boolean multiThreaded)
    {
        for (int i = 0; i <= ITERATIONS; i++)
        {
            resetMonitoring();
            boolean direct = false;
            int capacity = 0;
            executeAllocateBufferIterations(direct,capacity,multiThreaded);
            logMonitoringResults();
        }
    }

    @Test
    @Ignore("Slow as each time maxDirectMemory is full a fullGC is being triggered. See Bits.java line 624")
    public void directAllocationRandomCapacityTest()
    {
        directAllocationRandomCapacityIterations(false);
    }

    @Test
    @Ignore("Slow as each time maxDirectMemory is full a fullGC is being triggered. See Bits.java line 624")
    public void multiThreadedDirectAllocationRandomCapacityTest()
    {
        directAllocationRandomCapacityIterations(true);
    }

    private void directAllocationRandomCapacityIterations(boolean multiThreaded)
    {
        for (int i = 0; i <= ITERATIONS; i++)
        {
            resetMonitoring();
            boolean direct = true;
            int capacity = 0;
            executeAllocateBufferIterations(direct,capacity,multiThreaded);
            logMonitoringResults();
        }
    }

    @Test
    public void pooledBufferRandomCapacityTest()
    {
        pooledBufferRandomCapacityIterations(false);
    }

    @Test
    public void multiThreadedPooledBufferRandomCapacityTest()
    {
        pooledBufferRandomCapacityIterations(true);
    }

    private void pooledBufferRandomCapacityIterations(boolean multiThreaded)
    {
        for (int i = 0; i <= ITERATIONS; i++)
        {
            resetMonitoring();
            int capacity = 0;
            PooledBuffers bufferPool = new PooledBuffers(Buffers.Type.INDIRECT,capacity,Buffers.Type.INDIRECT,capacity,Buffers.Type.INDIRECT,capacity);
            executePooledBufferIterations(capacity,bufferPool,multiThreaded);
            logMonitoringResults();
        }
    }

    @Test
    @Ignore("Slow as each time maxDirectMemory is full a fullGC is being triggered. See Bits.java line 624")
    public void pooledDirectBufferRandomCapacityTest()
    {
        pooledDirectBufferRandomCapacityIterations(false);
    }

    private void pooledDirectBufferRandomCapacityIterations(boolean multiThreaded)
    {
        for (int i = 0; i <= ITERATIONS; i++)
        {
            resetMonitoring();
            int capacity = 0;
            PooledBuffers bufferPool = new PooledBuffers(Buffers.Type.DIRECT,capacity,Buffers.Type.DIRECT,capacity,Buffers.Type.DIRECT,capacity);
            executePooledBufferIterations(capacity,bufferPool,multiThreaded);
            logMonitoringResults();
        }
    }

    private void executePooledBufferIterations(final int capacity, final PooledBuffers bufferPool, boolean multithreaded)
    {
        for (int i = 0; i < NUMBER_ALLOCATIONS; i++)
        {
            if (multithreaded)
                threadPool.execute(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        allocatePooledBuffer(capacity,bufferPool);
                    }
                });
            else
                allocatePooledBuffer(capacity,bufferPool);
        }
        if (multithreaded)
            resetThreadPool();
    }

    private void allocatePooledBuffer(int capacity, PooledBuffers bufferPool)
    {
        if (capacity == 0)
            capacity = new Random().nextInt(7841) + 256;
        Buffer buffer = bufferPool.getBuffer(capacity);
        bufferPool.returnBuffer(buffer);
    }

    private void executeAllocateBufferIterations(final boolean direct, final int capacity, boolean multithreaded)
    {
        for (int i = 0; i < NUMBER_ALLOCATIONS; i++)
        {
            if (multithreaded)
                threadPool.execute(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            allocateBuffer(direct,capacity);
                        }
                        catch (Throwable e)
                        {
                            e.printStackTrace();
                        }
                    }
                });
            else
                allocateBuffer(direct,capacity);
        }
        if (multithreaded)
            resetThreadPool();
    }

    private void resetMonitoring()
    {
        System.gc(); // cleanup so each test starts with the same free memory
        youngCollectionTimeBefore = youngCollector.getCollectionTime();
        oldCollectionTimeBefore = oldCollector.getCollectionTime();
        startTime = System.currentTimeMillis();
        edenUsed = 0;
        oldGenUsed = 0;
        codeCacheUsed = 0;
        permGenSpaceUsed = 0;
        totalMemUsed = 0;
    }

    private void logMonitoringResults()
    {
        System.gc();
        long timeSpentInYoungCollection = youngCollector.getCollectionTime() - youngCollectionTimeBefore;
        long timeSpentInOldCollection = oldCollector.getCollectionTime() - oldCollectionTimeBefore;
        long overallTimeSpendInGc = timeSpentInOldCollection + timeSpentInYoungCollection;
        long timeTaken = System.currentTimeMillis() - startTime;
        float timeTakenInSeconds = timeTaken / 1000f;
        float allocationsPerSecond = NUMBER_ALLOCATIONS / timeTakenInSeconds;
        float edenKB = byteToKiloByte(edenUsed);
        float oldGenKB = byteToKiloByte(oldGenUsed);
        float totalMemUsedKB = byteToKiloByte(totalMemUsed);
        float survivorUsedKB = byteToKiloByte(survivorUsed);
        float codeCacheUsedKB = byteToKiloByte(codeCacheUsed);
        float permGenUsedKB = byteToKiloByte(permGenSpaceUsed);
        System.out.printf(getCurrentMethodName() + ": %1$5dms allocationsPerSecond: %2$15.2f",timeTaken,allocationsPerSecond);
        System.out.printf(" youngCollectionTime: %1$5dms oldCollectionTime: %2$5dms",timeSpentInYoungCollection,timeSpentInOldCollection);
        System.out.printf(" timeGC: " + overallTimeSpendInGc + "ms");
        System.out.printf(" eden: %1$8.0fkb oldGen: %2$8.0fkb total: %3$8.0fkb",edenKB,oldGenKB,
                totalMemUsedKB);
//        System.out.printf(" surv: %4$8.0fkb code: %5$8.0fkb, perm: %6$8.0fkb",survivorUsedKB,codeCacheUsedKB,permGenUsedKB);
        System.out.println();
    }

    private void resetThreadPool()
    {
        threadPool.shutdown();
        try
        {
            threadPool.awaitTermination(1,TimeUnit.MINUTES);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        resetThreadPoolExecutor();
    }

    private void allocateBuffer(boolean direct, int capacity)
    {
        if (capacity == 0)
            capacity = new Random().nextInt(7841) + 256;
        ByteBuffer buffer;
        if (direct)
        {
            buffer = ByteBuffer.allocateDirect(capacity);
        }
        else
        {
            buffer = ByteBuffer.allocate(capacity);
        }
        buffer.clear();
    }

    private String getCurrentMethodName()
    {
        return Thread.currentThread().getStackTrace()[4].getMethodName();
    }

    public float byteToMegaByte(long bytes)
    {
        return (float)bytes / 1024 / 1024;
    }

    public float byteToKiloByte(long bytes)
    {
        return (float)bytes / 1024;
    }
}
