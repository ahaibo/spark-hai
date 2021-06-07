//package com.hai.bgdata.spark.scala.akka
//
//import akka.actor.Actor
//
///**
// * description
// *
// * @author hai
// * @date 2021/6/5 15:43
// */
//object ActorTest {
//
//  class MyActor extends Actor {
//    override def receive: Receive = {
//      case string: String => {
//        println("receive: " + string)
//      }
//      case _ => println("default...")
//    }
//  }
//
//  def main(args: Array[String]): Unit = {
//    val actor1 = new MyActor()
//  }
//}
