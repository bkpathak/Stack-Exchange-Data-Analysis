package com.cloudwick.spark

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.SparkContext._
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by bijay on 3/3/15.
 */
case class Users(reputation: Int, creationDate: String, displayName: String, age: Int)

object Users {

  //Regex to find reputaion, creation date, display name and age
  val Regex ="""^.+Reputation="(\d+)" CreationDate="(\d{4}-\d{2}-\d{2})T.+" DisplayName="([\w\._ ]*).+Age="(\d+)".+$""".r

  def getDisplayNameReputation(row: String) = row match {
    case Regex(reputation, creationDate, displayName, age) =>
      Some(Users(reputation.toInt, creationDate, displayName, age.toInt))
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


object StackExchangeAnalysis {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        "Usage: StackExchangeAnalysis <Users.xml Badges.xml> <numPartitions> ")
      System.exit(-1)
    }


    val sparkConf = new SparkConf()
    //set conf to use Kryo Serializer

    sparkConf.set("spark.kryo.registrator", "com.cloudwick.spark.MyKryoRegistrator")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.rdd.compress", "true")

    sparkConf.setAppName("StackExchangeAnalyzer")

    // log configuration when applicatkion starts
    sparkConf.set("spark.logConf", "true")

    //create new spark context
    val sc = new SparkContext(sparkConf)

    //read the input file and number of partitions
    val users = sc.textFile(args(0))
    val badges = sc.textFile(args(1))

    //  parse the base users RDD and the result is cache
    val usersInfo = users.flatMap(line => Users.getDisplayNameReputation(line.toString)).cache()

    //  top users based on their reputation
    val topUsers = usersInfo.map(data => data.reputation -> data.displayName)
      .sortByKey(ascending = false)

    println("Top 10 users according to reputation")
    topUsers.take(10).foreach(println)

    //all unique badges present
    val uniqueBadges = badges.flatMap(line => Badge.getIdBadge(line))
      .map(data => data.badgeName)
      .distinct()

    println("All the unique badge name present")
    uniqueBadges.collect().foreach(println)

    // 10 most older users in the group
    val olderUser = usersInfo.map(data => data.age -> data.displayName)
      .sortByKey(ascending = false)

    println("10 oldest users")
    olderUser.take(10).foreach(println)

    // number of users created per day
    val usersCreatedPerDay = usersInfo.map(data => data.creationDate -> 1)
      .reduceByKey(_ + _)

    println("Numbers of user created per day")
    usersCreatedPerDay.collect().foreach(println)

  }
}