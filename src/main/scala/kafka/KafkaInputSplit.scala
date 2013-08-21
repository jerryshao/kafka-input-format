package kafka

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.InputSplit

class KafkaInputSplit(var host: String,
                      var port: Int,
                      var broker: Int,
                      var topic: String,
                      var partition: Int,
                      var offset: Long
  ) extends org.apache.hadoop.mapreduce.InputSplit with InputSplit {

  def this() = this(null, -1, -1, null, -1, 0)

  override def getLength(): Long = -1

  override def getLocations(): Array[String] = Array(host)

  override def readFields(in: DataInput) {
    val hostText = new Text()
    hostText.readFields(in)
    host = hostText.toString()

    port = in.readInt()
    broker = in.readInt()

    val topicText = new Text()
    topicText.readFields(in)
    topic = topicText.toString()

    partition = in.readInt()
    offset = in.readLong()
  }

  override def write(out: DataOutput) {
    val hostText = new Text(host)
    hostText.write(out)
    out.writeInt(port)
    out.writeInt(broker)
    val topicText = new Text(topic)
    topicText.write(out)
    out.writeInt(partition)
    out.writeLong(offset)
  }
}
