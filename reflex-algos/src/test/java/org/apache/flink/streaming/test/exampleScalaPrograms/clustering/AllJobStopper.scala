package org.apache.flink.streaming.test.exampleScalaPrograms.clustering

import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * a helper class to stop all jobs running on the local flink cluster
  * the helper runs as an independent thread and attempts to stop the
  * job until it succeeds.
  *
  * @param env Execution environment
  * @param cluster local cluster
  * @param timeoutMsec Wait for a certain duration before attempting to stop jobs
  */
class AllJobStopper(env: StreamExecutionEnvironment,
                    cluster: LocalFlinkMiniCluster,
                    timeoutMsec: Long) extends Runnable {
  var stopFlag = false
  override def run(): Unit = {
    Thread.sleep(timeoutMsec)

    var count = 0

    do {
      count = 0
      val jobsList = cluster.currentlyRunningJobs.toIterator
      while(jobsList.hasNext){
        val nextJob = jobsList.next()
        cluster.stopJob(nextJob)
        count += 1
      }
      Thread.sleep(1000)
    } while(count > 0 || !stopFlag)
  }

  def stop(): Unit = {
    stopFlag = true
  }
}

/**
  * Helper object to work with job stopper class above
  */
object AllJobStopper {

  /* store thread object to later use to wait for completion */
  var jsThread:Thread = _
  /* job stopper object */
  var jsObject:AllJobStopper = _

  /**
    * start a new job-stopper thread - this thread wait for a specified duration
    * and then start killing flink jobs
    * @param env stream environment
    * @param cluster flink cluster object
    * @param timeoutMsec time to wait before starting to stop
    */
  def start(env: StreamExecutionEnvironment,
            cluster: LocalFlinkMiniCluster,
            timeoutMsec: Long): Unit = {
    jsObject = new AllJobStopper(env, cluster, timeoutMsec)

    jsThread = new Thread(jsObject)

    jsThread.start()
  }

  /**
    * stop the job-stoppe thread
    */
  def stop() = {
    jsObject.stop()
    jsThread.join()
  }
}
