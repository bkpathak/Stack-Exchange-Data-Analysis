package com.cloudwick.spark

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by bijay on 3/3/15.
 */
case class NameRep(rep: Int, name: String)

object TopUsers {

  def extractNameRep(row: String) = {
    // Matches Reputation and Display Name.
    val repNameRegex = """Reputation="(\d+)".+DisplayName="([A-Za-z0-9_ ]*)"""".r;
    val nameRep = repNameRegex findFirstMatchIn (row) match {
      case Some(m) => new NameRep(m.group(1).toInt, m.group(2))
      // If match is not found or line returns None
      case _ => None
    }
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        "Usage: TopUsers <inputFile> <numPartitions> ")
      System.exit(-1)
    }
    val sparkConf = new SparkConf()
    sparkConf.setAppName("StackExchangeAnalyzer")
    sparkConf.setMaster("local[4]")

    val inputFile = args(0)
    val numPartitions = args(1).toInt

    val sc = new SparkContext(sparkConf)
    val usersXML = sc.textFile(inputFile, numPartitions)

    val topUsers = usersXML.map(line => extractNameRep(line.toString)).reduceByKey()
  }
}
