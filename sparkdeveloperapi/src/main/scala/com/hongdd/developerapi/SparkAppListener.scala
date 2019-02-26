package com.hongdd.developerapi

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._

class SparkAppListener extends SparkListener with Logging{
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    print("************************************")
    print("app-end")
    print("************************************")
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    jobEnd.jobResult match {
      case JobSucceeded => print("job-end-success")
//      case JobFailed(exception) =>
//        print("job-end-failed")
//        exception.printStackTrace()
    }
  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {
//    taskGettingResult.taskInfo.attemptNumber
  }
}
