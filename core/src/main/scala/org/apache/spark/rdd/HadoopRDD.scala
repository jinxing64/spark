/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import java.io.IOException
import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import scala.collection.immutable.Map
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapred.lib.CombineFileSplit
import org.apache.hadoop.mapreduce.TaskType
import org.apache.hadoop.util.ReflectionUtils

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.rdd.HadoopRDD.HadoopMapPartitionsWithSplitRDD
import org.apache.spark.scheduler.{HDFSCacheTaskLocation, HostTaskLocation}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{NextIterator, SerializableConfiguration, ShutdownHookManager}

/**
 * A Spark split class that wraps around a Hadoop InputSplit.
 */
private[spark] class HadoopPartition(rddId: Int, override val index: Int, splits: Array[InputSplit])
  extends Partition {

  assert(splits.nonEmpty, "There should be at least one partition in hadoop partition.")
  val inputSplits = new SerializableWritable2(splits)

  override def hashCode(): Int = 31 * (31 + rddId) + index

  override def equals(other: Any): Boolean = super.equals(other)

  /**
   * Get any environment variables that should be added to the users environment when running pipes
   * @return a Map with the environment variables and corresponding values, it could be empty
   */
  def getPipeEnvVars(): Map[String, String] = {
    val envVars: Map[String, String] = if (inputSplits.value.head.isInstanceOf[FileSplit]) {
      val fileSplits = inputSplits.value.map(_.asInstanceOf[FileSplit])
      // map_input_file is deprecated in favor of mapreduce_map_input_file but set both
      // since it's not removed yet
      Map("map_input_file" -> fileSplits.map(_.getPath().toString()).mkString(","),
        "mapreduce_map_input_file" -> fileSplits.map(_.getPath().toString()).mkString(","))
    } else {
      Map()
    }
    envVars
  }
}

/**
 * :: DeveloperApi ::
 * An RDD that provides core functionality for reading data stored in Hadoop (e.g., files in HDFS,
 * sources in HBase, or S3), using the older MapReduce API (`org.apache.hadoop.mapred`).
 *
 * @param sc The SparkContext to associate the RDD with.
 * @param broadcastedConf A general Hadoop Configuration, or a subclass of it. If the enclosed
 *   variable references an instance of JobConf, then that JobConf will be used for the Hadoop job.
 *   Otherwise, a new JobConf will be created on each slave using the enclosed Configuration.
 * @param initLocalJobConfFuncOpt Optional closure used to initialize any JobConf that HadoopRDD
 *     creates.
 * @param inputFormatClass Storage format of the data to be read.
 * @param keyClass Class of the key associated with the inputFormatClass.
 * @param valueClass Class of the value associated with the inputFormatClass.
 * @param minPartitions Minimum number of HadoopRDD partitions (Hadoop Splits) to generate.
 *
 * @note Instantiating this class directly is not recommended, please use
 * `org.apache.spark.SparkContext.hadoopRDD()`
 */
@DeveloperApi
class HadoopRDD[K, V](
    sc: SparkContext,
    broadcastedConf: Broadcast[SerializableConfiguration],
    initLocalJobConfFuncOpt: Option[JobConf => Unit],
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int)
  extends RDD[(K, V)](sc, Nil) with Logging {

  if (initLocalJobConfFuncOpt.isDefined) {
    sparkContext.clean(initLocalJobConfFuncOpt.get)
  }

  def this(
      sc: SparkContext,
      conf: JobConf,
      inputFormatClass: Class[_ <: InputFormat[K, V]],
      keyClass: Class[K],
      valueClass: Class[V],
      minPartitions: Int) = {
    this(
      sc,
      sc.broadcast(new SerializableConfiguration(conf))
        .asInstanceOf[Broadcast[SerializableConfiguration]],
      initLocalJobConfFuncOpt = None,
      inputFormatClass,
      keyClass,
      valueClass,
      minPartitions)
  }

  protected val jobConfCacheKey: String = "rdd_%d_job_conf".format(id)

  protected val inputFormatCacheKey: String = "rdd_%d_input_format".format(id)

  // used to build JobTracker ID
  private val createTime = new Date()

  private val shouldCloneJobConf = sparkContext.conf.getBoolean("spark.hadoop.cloneConf", false)

  private val ignoreCorruptFiles = sparkContext.conf.get(IGNORE_CORRUPT_FILES)

  private val ignoreEmptySplits = sparkContext.conf.get(HADOOP_RDD_IGNORE_EMPTY_SPLITS)

  private val maxPartitions = sparkContext.conf.get(HADOOP_RDD_MAX_PARTITIONS)

  private val maxBytesInPartition = sparkContext.conf.get(HADOOP_RDD_MAX_BYTES_IN_PARTITION)

  // Returns a JobConf that will be used on slaves to obtain input splits for Hadoop reads.
  protected def getJobConf(): JobConf = {
    val conf: Configuration = broadcastedConf.value.value
    if (shouldCloneJobConf) {
      // Hadoop Configuration objects are not thread-safe, which may lead to various problems if
      // one job modifies a configuration while another reads it (SPARK-2546).  This problem occurs
      // somewhat rarely because most jobs treat the configuration as though it's immutable.  One
      // solution, implemented here, is to clone the Configuration object.  Unfortunately, this
      // clone can be very expensive.  To avoid unexpected performance regressions for workloads and
      // Hadoop versions that do not suffer from these thread-safety issues, this cloning is
      // disabled by default.
      HadoopRDD.CONFIGURATION_INSTANTIATION_LOCK.synchronized {
        logDebug("Cloning Hadoop Configuration")
        val newJobConf = new JobConf(conf)
        if (!conf.isInstanceOf[JobConf]) {
          initLocalJobConfFuncOpt.foreach(f => f(newJobConf))
        }
        newJobConf
      }
    } else {
      if (conf.isInstanceOf[JobConf]) {
        logDebug("Re-using user-broadcasted JobConf")
        conf.asInstanceOf[JobConf]
      } else {
        Option(HadoopRDD.getCachedMetadata(jobConfCacheKey))
          .map { conf =>
            logDebug("Re-using cached JobConf")
            conf.asInstanceOf[JobConf]
          }
          .getOrElse {
            // Create a JobConf that will be cached and used across this RDD's getJobConf() calls in
            // the local process. The local cache is accessed through HadoopRDD.putCachedMetadata().
            // The caching helps minimize GC, since a JobConf can contain ~10KB of temporary
            // objects. Synchronize to prevent ConcurrentModificationException (SPARK-1097,
            // HADOOP-10456).
            HadoopRDD.CONFIGURATION_INSTANTIATION_LOCK.synchronized {
              logDebug("Creating new JobConf and caching it for later re-use")
              val newJobConf = new JobConf(conf)
              initLocalJobConfFuncOpt.foreach(f => f(newJobConf))
              HadoopRDD.putCachedMetadata(jobConfCacheKey, newJobConf)
              newJobConf
          }
        }
      }
    }
  }

  protected def getInputFormat(conf: JobConf): InputFormat[K, V] = {
    val newInputFormat = ReflectionUtils.newInstance(inputFormatClass.asInstanceOf[Class[_]], conf)
      .asInstanceOf[InputFormat[K, V]]
    newInputFormat match {
      case c: Configurable => c.setConf(conf)
      case _ =>
    }
    newInputFormat
  }

  override def getPartitions: Array[Partition] = {
    val jobConf = getJobConf()
    // add the credentials here as this can be called before SparkContext initialized
    SparkHadoopUtil.get.addCredentials(jobConf)
    val allInputSplits = getInputFormat(jobConf).getSplits(jobConf, minPartitions)
    val inputSplits = if (ignoreEmptySplits) {
      allInputSplits.filter(_.getLength > 0)
    } else {
      allInputSplits
    }

    if (inputSplits.size < maxPartitions) {
      val arrayBuffer = ArrayBuffer[Partition]()
      var splitsBuffer = ArrayBuffer[InputSplit]()
      var idx = 0
      for (i <- 0 until inputSplits.size) {
        if (splitsBuffer.nonEmpty &&
          splitsBuffer.map(_.getLength).sum + inputSplits(i).getLength > maxBytesInPartition) {
          arrayBuffer += new HadoopPartition(id, idx, splitsBuffer.toArray)
          idx += 1
          splitsBuffer.clear()
        }
        splitsBuffer += inputSplits(i)
      }
      if (splitsBuffer.nonEmpty) {
        arrayBuffer += new HadoopPartition(id, idx, splitsBuffer.toArray)
        idx += 1 // no necessary
      }
      arrayBuffer.toArray
    } else {
      val arrayBuffer = ArrayBuffer[Partition]()
      val avg = inputSplits.size / maxPartitions + 1
      var splitsBuffer = ArrayBuffer[InputSplit]()
      var idx = 0
      for (i <- 0 until inputSplits.size) {
        if (splitsBuffer.nonEmpty && splitsBuffer.size >= avg &&
          splitsBuffer.map(_.getLength).sum + inputSplits(i).getLength > maxBytesInPartition) {
          arrayBuffer += new HadoopPartition(id, idx, splitsBuffer.toArray)
          idx += 1
          splitsBuffer.clear()
        }
        splitsBuffer += inputSplits(i)
      }
      if (splitsBuffer.nonEmpty) {
        arrayBuffer += new HadoopPartition(id, idx, splitsBuffer.toArray)
        idx += 1 // no necessary
      }

      arrayBuffer.toArray
    }
  }

  override def compute(part: Partition, context: TaskContext): InterruptibleIterator[(K, V)] = {
    val iter = new NextIterator[(K, V)] {

      private val hadoopPartition = part.asInstanceOf[HadoopPartition]
      private val splits = hadoopPartition.inputSplits.value
      logInfo("Input split: " + splits)
      private val jobConf = getJobConf()

      private val inputMetrics = context.taskMetrics().inputMetrics
      private val existingBytesRead = inputMetrics.bytesRead

      private var curSplitIdx = 0
      private def curSplit = splits(curSplitIdx)

      def setupInputFileBlockHolder(): Unit = {
        // Sets InputFileBlockHolder for the file block's information
        curSplit match {
          case fs: FileSplit =>
            InputFileBlockHolder.set(fs.getPath.toString, fs.getStart, fs.getLength)
          case _ =>
            InputFileBlockHolder.unset()
        }
      }
      setupInputFileBlockHolder()

      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // creating RecordReader, because RecordReader's constructor might read some bytes
      private val getBytesReadCallback: Option[() => Long] = splits.head match {
        case _: FileSplit | _: CombineFileSplit =>
          Some(SparkHadoopUtil.get.getFSBytesReadOnThreadCallback())
        case _ => None
      }

      // We get our input bytes from thread-local Hadoop FileSystem statistics.
      // If we do a coalesce, however, we are likely to compute multiple partitions in the same
      // task and in the same thread, in which case we need to avoid override values written by
      // previous partitions (SPARK-13071).
      private def updateBytesRead(): Unit = {
        getBytesReadCallback.foreach { getBytesRead =>
          inputMetrics.setBytesRead(existingBytesRead + getBytesRead())
        }
      }

      private def updateReader(): Unit = {
        reader =
          try {
            inputFormat.getRecordReader(curSplit, jobConf, Reporter.NULL)
          } catch {
            case e: IOException if ignoreCorruptFiles =>
              logWarning(s"Skipped the rest content in the corrupted files: " +
                s"${splits.slice(curSplitIdx, splits.length)}", e)
              finished = true
              null
          }
      }

      private var reader: RecordReader[K, V] = null
      private val inputFormat = getInputFormat(jobConf)
      updateReader()
      HadoopRDD.addLocalConfiguration(
        new SimpleDateFormat("yyyyMMddHHmmss", Locale.US).format(createTime),
        context.stageId, hadoopPartition.index, context.attemptNumber, jobConf)

      // Register an on-task-completion callback to close the input stream.
      context.addTaskCompletionListener { context =>
        // Update the bytes read before closing is to make sure lingering bytesRead statistics in
        // this thread get correctly added.
        updateBytesRead()
        closeIfNeeded()
      }

      private val key: K = if (reader == null) null.asInstanceOf[K] else reader.createKey()
      private val value: V = if (reader == null) null.asInstanceOf[V] else reader.createValue()

      override def getNext(): (K, V) = {
        try {
          var readerFed = false
          while (!finished && !readerFed) {
            if (reader.next(key, value)) {
              readerFed = true
            } else {
              reader.close()
              curSplitIdx += 1
              if (curSplitIdx < splits.length) {
                updateReader()
                setupInputFileBlockHolder()
              } else {
                finished = true
              }
            }
          }
        } catch {
          case e: IOException if ignoreCorruptFiles =>
            logWarning(s"Skipped the rest content in the corrupted files: " +
              s"${splits.slice(curSplitIdx, splits.length)}", e)
            finished = true
        }
        if (!finished) {
          inputMetrics.incRecordsRead(1)
        }
        if (inputMetrics.recordsRead % SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0) {
          updateBytesRead()
        }
        (key, value)
      }

      override def close(): Unit = {
        if (reader != null) {
          InputFileBlockHolder.unset()
          try {
            reader.close()
          } catch {
            case e: Exception =>
              if (!ShutdownHookManager.inShutdown()) {
                logWarning("Exception in RecordReader.close()", e)
              }
          } finally {
            reader = null
          }
          if (getBytesReadCallback.isDefined) {
            updateBytesRead()
          } else if (splits.head.isInstanceOf[FileSplit] ||
                     splits.head.isInstanceOf[CombineFileSplit]) {
            // If we can't get the bytes read from the FS stats, fall back to the split size,
            // which may be inaccurate.
            try {
              inputMetrics.incBytesRead(splits.map(_.getLength).sum)
            } catch {
              case e: java.io.IOException =>
                logWarning("Unable to get input size to set InputMetrics for task", e)
            }
          }
        }
      }
    }
    new InterruptibleIterator[(K, V)](context, iter)
  }

  /** Maps over a partition, providing the InputSplit that was used as the base of the partition. */
  @DeveloperApi
  def mapPartitionsWithInputSplit[U: ClassTag](
      f: (Seq[InputSplit], Iterator[(K, V)]) => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] = {
    new HadoopMapPartitionsWithSplitRDD(this, f, preservesPartitioning)
  }

  override def getPreferredLocations(part: Partition): Seq[String] = {
    val hsplits = part.asInstanceOf[HadoopPartition].inputSplits.value
    val locs = hsplits.head match {
      case _: InputSplitWithLocationInfo =>
        val lsplits = hsplits.map(_.asInstanceOf[InputSplitWithLocationInfo])
        lsplits.map { split =>
          HadoopRDD.convertSplitLocationInfo(split.getLocationInfo)
        }.foldLeft(Option[Seq[String]](Seq())) {
          case (Some(a), Some(b)) => Some(a ++ b)
          case (None, b) => b
          case (a, None) => a
        }
      case _ => None
    }
    locs.getOrElse(hsplits.flatMap(_.getLocations).filter(_ != "localhost"))
  }

  override def checkpoint() {
    // Do nothing. Hadoop RDD should not be checkpointed.
  }

  override def persist(storageLevel: StorageLevel): this.type = {
    if (storageLevel.deserialized) {
      logWarning("Caching HadoopRDDs as deserialized objects usually leads to undesired" +
        " behavior because Hadoop's RecordReader reuses the same Writable object for all records." +
        " Use a map transformation to make copies of the records.")
    }
    super.persist(storageLevel)
  }

  def getConf: Configuration = getJobConf()
}

private[spark] object HadoopRDD extends Logging {
  /**
   * Configuration's constructor is not threadsafe (see SPARK-1097 and HADOOP-10456).
   * Therefore, we synchronize on this lock before calling new JobConf() or new Configuration().
   */
  val CONFIGURATION_INSTANTIATION_LOCK = new Object()

  /** Update the input bytes read metric each time this number of records has been read */
  val RECORDS_BETWEEN_BYTES_READ_METRIC_UPDATES = 256

  /**
   * The three methods below are helpers for accessing the local map, a property of the SparkEnv of
   * the local process.
   */
  def getCachedMetadata(key: String): Any = SparkEnv.get.hadoopJobMetadata.get(key)

  private def putCachedMetadata(key: String, value: Any): Unit =
    SparkEnv.get.hadoopJobMetadata.put(key, value)

  /** Add Hadoop configuration specific to a single partition and attempt. */
  def addLocalConfiguration(jobTrackerId: String, jobId: Int, splitId: Int, attemptId: Int,
                            conf: JobConf) {
    val jobID = new JobID(jobTrackerId, jobId)
    val taId = new TaskAttemptID(new TaskID(jobID, TaskType.MAP, splitId), attemptId)

    conf.set("mapreduce.task.id", taId.getTaskID.toString)
    conf.set("mapreduce.task.attempt.id", taId.toString)
    conf.setBoolean("mapreduce.task.ismap", true)
    conf.setInt("mapreduce.task.partition", splitId)
    conf.set("mapreduce.job.id", jobID.toString)
  }

  /**
   * Analogous to [[org.apache.spark.rdd.MapPartitionsRDD]], but passes in an InputSplit to
   * the given function rather than the index of the partition.
   */
  private[spark] class HadoopMapPartitionsWithSplitRDD[U: ClassTag, T: ClassTag](
      prev: RDD[T],
      f: (Seq[InputSplit], Iterator[T]) => Iterator[U],
      preservesPartitioning: Boolean = false)
    extends RDD[U](prev) {

    override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

    override def getPartitions: Array[Partition] = firstParent[T].partitions

    override def compute(part: Partition, context: TaskContext): Iterator[U] = {
      val partition = part.asInstanceOf[HadoopPartition]
      val inputSplits = partition.inputSplits.value
      f(inputSplits, firstParent[T].iterator(part, context))
    }
  }

  private[spark] def convertSplitLocationInfo(
       infos: Array[SplitLocationInfo]): Option[Seq[String]] = {
    Option(infos).map(_.flatMap { loc =>
      val locationStr = loc.getLocation
      if (locationStr != "localhost") {
        if (loc.isInMemory) {
          logDebug(s"Partition $locationStr is cached by Hadoop.")
          Some(HDFSCacheTaskLocation(locationStr).toString)
        } else {
          Some(HostTaskLocation(locationStr).toString)
        }
      } else {
        None
      }
    })
  }
}
