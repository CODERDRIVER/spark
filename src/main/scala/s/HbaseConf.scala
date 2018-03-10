package s

import org.apache.hadoop.conf.Configuration

/**
  * Author: cwz
  * Time: 2017/9/19
  * Description: 设置hbase连接配置
  */
object HbaseConf {
  // 配置hbase的主节点ip
  val master: String = "192.168.1.128"
  // 配置hbase的zookeeper节点ip
  val zookeeper: String = "192.168.1.128,192.168.1.129,192.168.1.130"
  // 配置hbase的zookeeper连接端口，默认值为2181
  val port = "2181"

  def getConf(): Configuration = {
    val hbaseConf: Configuration = new Configuration
    hbaseConf.set("hbase.master", master)
    hbaseConf.set("hbase.zookeeper.quorum", zookeeper)
    hbaseConf.set("hbase.zookeeper.property.clientPort", port)
    hbaseConf
  }
}
