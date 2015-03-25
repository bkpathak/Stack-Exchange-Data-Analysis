package com.cloudwick.spark

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.SparkContext._
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by bijay on 3/3/15.
 */
case class Users(reputation: Int, displayName: String, age: Int)

object Users {

  //Regex to find reputaion and display name
  val Regex = """Reputation="(\d+)".+DisplayName="([A-Za-z0-9_ ]*)".+Age="([0-9]{2})"""".r

  def getDisplayNameReputation(row: String) = row match {
    case Regex(reputation, displayName, age) => Some(Users(reputation.toInt, displayName, age.toInt))
    case _ => None
  }
}

case class Badge(userId: Int, badgeName: String)

object Badge {
  val Regex = """^.* UserId="([0-9]+)" Name="([a-zA-Z0-9]+)".*$""".r

  def getIdBadge(row: String) = row match {
    case Regex(userId, badgeName) => Some(Badge(userId.toInt, badgeName))
    case _ => None
  }
}

// Use Kryo instead of default Java Serilaization
class MyKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Badge])
    kryo.register(classOf[Users])
  }
}

object DataAnalyses {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        "Usage: StackExchangeAnalysis <Users.xml Badges.xml> <numPartitions> ")
      System.exit(-1)
    }


    val sparkConf = new SparkConf()
    //set conf to use Kryo Serializer

    sparkConf.set("spark.kryo.registrator", "sparking.MyKryoRegistrator")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.rdd.compress", "true")

    sparkConf.setAppName("StackExchangeAnalyzer")

    // log configuration when applicatkion starts
    sparkConf.set("spark.logConf", "true")

    //create new spark context
    val sc = new SparkContext(sparkConf)

    //read the input file and number of partitions, the second argument is number of partition to create
    val users = sc.textFile(args(0), args(2).toInt)
    val badges = sc.textFile(args(1), args(2).toInt)

    //  top users based on their reputation
    val topUsers = users.flatMap(Users.getDisplayNameReputation)
      .map(data => data.reputation -> data.displayName)
      .sortByKey()
    println("Top 10 users according to reputation")
    topUsers.take(10).foreach(println)
  }
}