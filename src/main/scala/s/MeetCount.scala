package s

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf

/**
  * Author: cwz
  * Time: 2017/9/19
  * Description: 统计每个地点经过的车次，输出结果到vehicle_count表中，行健为placeId，列族为info，列名为address,latitude,longitude,count
  */

object MeetCount {
  val columnFamilyName = "info"
  val inputTableName = "Record"
  val outputTableName = "MeetCount"

  def main(args: Array[String]): Unit = {
    if (HbaseUtils.createTable(outputTableName, Array(columnFamilyName))) {
      val sc = SC.getLocalSC("MeetCount")
      val inputHbaseConf = HbaseConf.getConf()
      inputHbaseConf.set(TableInputFormat.INPUT_TABLE, inputTableName)

      val outputHbaseConf = HbaseConf.getConf()
      val jobConf = new JobConf(outputHbaseConf)
      jobConf.setOutputFormat(classOf[TableOutputFormat])
      jobConf.set(TableOutputFormat.OUTPUT_TABLE, outputTableName)

      val tmp1 = sc.newAPIHadoopRDD(inputHbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
         .map { case (_, input) => {
          val row: String = Bytes.toString(input.getRow)
          val placeId: String = row.split("##")(0)
          val time: String= row.split("##")(1)
          val eid: String = row.split("##")(2)
          (placeId, ( eid, time))
        }}
      val tmp2 = tmp1.join(tmp1)
      tmp2.filter{ case (_,((eid1,time1),(eid2,time2))) => {
        eid1 != eid2 && math.abs(time1.toInt - time2.toInt) < 60
        }
      }
        .map{ case (_,((eid1,_),(eid2,_))) =>
          ((eid1,eid2),1)
      }
        .reduceByKey( _+_ )
        .map { case (placeInfo, count) => {
          val put = new Put(Bytes.toBytes(placeInfo._1))
          put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("eid"), Bytes.toBytes(placeInfo._2))
          put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("count"), Bytes.toBytes(count))
          (new ImmutableBytesWritable, put)
        }}

        .saveAsHadoopDataset(jobConf)

      sc.stop()
    }
  }
}