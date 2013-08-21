package kafka

import java.io.IOException

import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.mapred.{InputFormat,InputSplit, JobConf, Reporter, RecordReader}

import org.apache.zookeeper.ZooKeeper

class KafkaInputFormat extends InputFormat[KafkaInputKey, BytesWritable] {

  override def getSplits(job: JobConf, numSplits: Int): Array[InputSplit] = {
    var zk: ZooKeeper = null
    try {
      zk = new ZooKeeper(KafkaInputFormat.zkQuorum(job), KafkaInputFormat.SESSION_TIMEOUT, null)
    } catch {
      case e: Exception => println(e.getMessage)
    }

    val brokers = getBrokers(zk).toMap
    val partitions = getPartitions(zk, KafkaInputFormat.topic(job)).toMap

    val splits = partitions flatMap { kv =>
      val b = brokers(kv._1)
      (0 until kv._2) map { i =>
        val offset = getOffsets(zk, KafkaInputFormat.group(job), KafkaInputFormat.topic(job), kv._1, i)
        new KafkaInputSplit(b._1, b._2, kv._1, KafkaInputFormat.topic(job), i, offset)
      }
    }
    zk.close()

    splits.toArray
  }

  override def getRecordReader(split: InputSplit, job: JobConf, reporter: Reporter)
    : RecordReader[KafkaInputKey, BytesWritable] = {
    new KafkaRecordReader(split, job, reporter)
  }

  def getBrokers(zk: ZooKeeper) = {
    import scala.collection.JavaConversions._
    zk.getChildren(KafkaInputFormat.BROKERS_PATH, null) map { p =>
      val brokerInfo = new String(zk.getData(
        KafkaInputFormat.BROKERS_PATH + KafkaInputFormat.SEPARATOR + p, false, null))
      val splits = brokerInfo.split(KafkaInputFormat.DELIMITER)
      println("Got Brokers [" + p + "], HostPort[" + brokerInfo + "]")
      p.toInt -> (splits(1), splits(2).toInt)
    }
  }

  def getPartitions(zk: ZooKeeper, topic: String) = {
    val topicPath = KafkaInputFormat.TOPIC_PARTITIONS_PATH + KafkaInputFormat.SEPARATOR + topic
    import scala.collection.JavaConversions._

    zk.getChildren(topicPath, null) map { p =>
      val partitionInfo = new String(zk.getData(
        topicPath + KafkaInputFormat.SEPARATOR + p, false, null))
      println("Got Broker [" + p + "] partition info[" + partitionInfo + "]")
      p.toInt -> partitionInfo.toInt
    }
  }

  def getOffsets(zk: ZooKeeper, group: String, topic: String, brokerId: Int, part: Int) = {
    val path = KafkaInputFormat.CONSUMERS + KafkaInputFormat.SEPARATOR + group +
      KafkaInputFormat.SEPARATOR + "offsets" + KafkaInputFormat.SEPARATOR +
      topic + KafkaInputFormat.SEPARATOR + brokerId + "-" + part

    val offset = try {
      new String(zk.getData(path, false, null)).toLong
    } catch {
      case _ => 0
    }
    println("Get topic: " + topic + " group: " + group + " brokerId: " + brokerId + " part: " +
      part + " offset:" + offset)

    offset
  }
}

object KafkaInputFormat {
  val TOPIC_PARTITIONS_PATH = "/brokers/topics"
  val BROKERS_PATH = "/brokers/ids"
  val CONSUMERS = "/custom/consumers"
  val SESSION_TIMEOUT = 10000
  val SEPARATOR = "/"
  val DELIMITER = ":"

  def zkQuorum(conf: JobConf) = Option(conf.get("zookeeper.zkquorum")).getOrElse("localhost:2181")

  def topic(conf: JobConf) = Option(conf.get("kafka.topic")) match {
    case Some(t) => t
    case None => throw new IOException("kafka.topic not set")
  }

  def group(conf: JobConf) = Option(conf.get("kafka.group")) match {
    case Some(g) => g
    case None => throw new IOException("kafka.group not set")
  }
}
