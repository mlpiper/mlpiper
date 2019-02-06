package org.apache.flink.streaming.test.exampleScalaPrograms.clustering

import org.apache.flink.api.common.JobID
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster
import org.apache.flink.streaming.util.TestStreamEnvironment
import org.apache.flink.test.util.{AbstractTestBase, TestBaseUtils}
import org.junit.{After, Before}

/**
  * An extension to Flink Mini Cluster. This exposes mechanisms to stop jobs
  * which can be used by our test routines to stop test streams
  *
  * @param config Configuration - typically empty
  */
class PMMiniCluster(config: Configuration) extends AbstractTestBase(config) {
  protected val DEFAULT_PARALLELISM = 4
  protected var cluster: LocalFlinkMiniCluster = _

  def this() = {
    this(new Configuration())
  }

  @Before
  def setup(): Unit = {
    cluster = TestBaseUtils.startCluster(1, DEFAULT_PARALLELISM, false, false, true)
    TestStreamEnvironment.setAsContext(cluster, DEFAULT_PARALLELISM)
  }

  @After
  def teardown(): Unit = {
    TestStreamEnvironment.unsetAsContext()
    TestBaseUtils.stopCluster(cluster, TestBaseUtils.DEFAULT_TIMEOUT)
  }

  def stopJob(id: JobID): Unit = {
    cluster.stopJob(id)
  }
}


