package kafka

import java.io.IOException

import kafka.api.{FetchRequest, OffsetRequest}
import kafka.consumer.SimpleConsumer
import kafka.common.ErrorMapping
import kafka.message.{ByteBufferMessageSet, MessageAndOffset}

import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.mapred.{InputSplit, JobConf, RecordReader, Reporter}

import org.apache.zookeeper.{CreateMode, ZooKeeper}
import org.apache.zookeeper.ZooDefs.Ids

class KafkaRecordReader(val inputSplit: InputSplit, val job: JobConf, val reporter: Reporter)
  extends RecordReader[KafkaInputKey, BytesWritable] {
  val SO_TIMEOUT = 10000
  val SO_BUFFER_SIZE = 16 * 1024 * 1024
  val split = inputSplit.asInstanceOf[KafkaInputSplit]

  val consumer = new SimpleConsumer(split.host, split.port, SO_TIMEOUT, SO_BUFFER_SIZE)

  // Get latest available offset
  val(leastOffset, latestOffset) = getOffset
  if (split.offset < leastOffset) {
    println("WARN: least available offset: " + leastOffset + " is larger than last offset: " +
    split.offset + ". Setting last offset to least available offset")
    split.offset = leastOffset
  }

  if (split.offset > latestOffset) {
    println("WARN: latest offset: " + latestOffset + " is small than last offset: " +
      split.offset + ". Setting last offset to least available offset.")
    split.offset = leastOffset
  }

  /**
   * Write the target offset ahead of time, to ensure next job will start without reading
   * duplicated data when current job is not finished yet.
   */
  writeOffset(latestOffset)

  @volatile var currentOffset = split.offset
  var messageIter: Iterator[MessageAndOffset] = null

  override def next(key: KafkaInputKey, value: BytesWritable): Boolean = synchronized {
    if (!hasMore) return false

    if (messageIter == null || !messageIter.hasNext) {
      val fetchRequest = new FetchRequest(split.topic,
        split.partition,
        currentOffset,
        SO_BUFFER_SIZE)

      val messages = consumer.fetch(fetchRequest)
      if (hasError(messages)) return false

      messageIter = messages.iterator
    }

    // Fetch the data from each message and put to key, value
    val msgAndOffset = messageIter.next()
    val buf = msgAndOffset.message.payload
    val len = msgAndOffset.message.payloadSize
    val bytes = new Array[Byte](len)
    buf.get(bytes, buf.position(), len)
    value.set(bytes, 0, len)

    key.set(split.partition, currentOffset, msgAndOffset.message.checksum)

    currentOffset = msgAndOffset.offset
    true
  }

  override def getPos(): Long = currentOffset

  override def createKey(): KafkaInputKey = new KafkaInputKey()

  override def createValue(): BytesWritable = new BytesWritable()

  override def close(): Unit = synchronized {
    consumer.close()
  }

  override def getProgress(): Float = {
    val lastOffset = split.offset
    val total = latestOffset - lastOffset

    (currentOffset - lastOffset) / total.toFloat
  }

  private def createParentNodes(path: String, zk: ZooKeeper) {
    val parents = path.split(KafkaInputFormat.SEPARATOR).drop(1).dropRight(1).
      scanLeft("")(_ + "/" + _).drop(1)

    parents foreach { p =>
      Option(zk.exists(p, false)) match {
        case Some(s) => Unit
        case None => zk.create(p, new Array[Byte](0), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      }
    }
  }

  def getOffset: (Long, Long) = {
    val latestOffsets = consumer.getOffsetsBefore(
      split.topic, split.partition, OffsetRequest.LatestTime, 1)
    assert(latestOffsets.length == 1)

    val leastOffsets = consumer.getOffsetsBefore(
      split.topic, split.partition, OffsetRequest.EarliestTime, 1)
    assert(leastOffsets.length == 1)

    println("Got least available offset: " + leastOffsets(0) + " for topic [" + split.topic +
      "] partition [" + split.partition + "]")
    println("Got latest offset: " + latestOffsets(0) + " for topic [" + split.topic +
      "] partition [" + split.partition + "]")

    (leastOffsets(0), latestOffsets(0))
  }

  def hasMore: Boolean = (messageIter != null && messageIter.hasNext) ||
    (currentOffset < latestOffset)

  def hasError(messages: ByteBufferMessageSet): Boolean = {
    val errorCode = messages.getErrorCode

    if (errorCode == ErrorMapping.OffsetOutOfRangeCode) {
      println("WARN: current offset:" + currentOffset + " is out of range")
      true
    } else if (errorCode == ErrorMapping.InvalidMessageCode) {
      throw new IOException("current message is invalid, offset: " + currentOffset)
    } else if (errorCode == ErrorMapping.WrongPartitionCode) {
      throw new IOException("current partition is invalid, partition " + split.partition)
    } else if (errorCode != ErrorMapping.NoError) {
      throw new IOException("current offset: " + currentOffset + " error: " + errorCode)
    } else {
      false
    }
  }

  def writeOffset(offset: Long): Unit = {
    // Write current offset to zookeeper for next time use
    val bytes = offset.toString.getBytes
    var zk: ZooKeeper = null
    try {
      zk = new ZooKeeper(KafkaInputFormat.zkQuorum(job), KafkaInputFormat.SESSION_TIMEOUT, null)

      val offsetPath = KafkaInputFormat.CONSUMERS + KafkaInputFormat.SEPARATOR +
        KafkaInputFormat.group(job) + KafkaInputFormat.SEPARATOR +
        "offsets" + KafkaInputFormat.SEPARATOR + split.topic + KafkaInputFormat.SEPARATOR +
        split.broker + "-" + split.partition

      println("Write current offset: " + offset + " to file [" + offsetPath + "]")
      Option(zk.exists(offsetPath, false)) match {
        case Some(s) => zk.setData(offsetPath, bytes, -1)
        case None =>
          createParentNodes(offsetPath, zk)
          zk.create(offsetPath, bytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      }
    } catch {
      case e: Exception => println(e.getMessage)
    } finally {
      zk.close()
    }

  }
}
