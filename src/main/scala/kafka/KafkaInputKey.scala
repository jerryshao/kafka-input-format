package kafka

import java.io.{DataInput, DataOutput}
import org.apache.hadoop.io.WritableComparable

class KafkaInputKey(var inputIndex: Int, var offset: Long, var checksum: Long)
  extends WritableComparable[KafkaInputKey] {

  def this() = this(0, 0, 0)
  def this(index: Int, offset: Long) = this(index, offset, 0)

  def set(index: Int, offset: Long, chksum: Long) {
    this.inputIndex = index
    this.offset = offset
    this.checksum = chksum
  }

  override def readFields(in: DataInput) {
    inputIndex = in.readInt()
    offset = in.readLong()
    checksum = in.readLong()
  }

  override def write(out: DataOutput) {
    out.writeInt(inputIndex)
    out.writeLong(offset)
    out.writeLong(checksum)
  }

  override def compareTo(other: KafkaInputKey): Int = {
    if (inputIndex != other.inputIndex) {
      inputIndex - other.inputIndex
    } else {
      if (offset != other.offset) {
        (offset - other.offset).toInt
      } else {
        (checksum - other.checksum).toInt
      }
    }
  }

  override def toString() = "KafkaKey inputIndex: " + inputIndex +
    " offset: " + offset + " checksum: " + checksum
}
