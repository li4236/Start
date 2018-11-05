//package test;
//
//import akka.http.scaladsl.model.DateTime;
//import reactivemongo.api.MongoDriver;
//import reactivemongo.bson.BSONObjectID;
//import scala.Unit;
//
//class RMongoStudy {
//
//    // 获得mongo driver实例(creates an actor system)
//    val driver = new MongoDriver()
//    val connection = driver.connection(List("localhost"))// 参数是一个节点名称List, 例如"node1.foo.com:27017"， 端口号是可选的,默认是27017.
//
//    // 获得数据库"mygame"的引用
//    val db = connection("mygame")//参数是db name, 执行插入或更新操作时若不存在会自动创建
//
//    // 获得collection"players"的引用，默认是一个BSONCollection
//    val playerCollection = db("players")  //参数是collection(table) name, 执行插入或更新操作时若不存在会自动创建
//
//    //增
//    def createTest() : Unit= {
//
//        //创建一个新的文档，用来插入
//        val newPlayerDoc = BSONDocument(
//                "_id" -> BSONObjectID.generate,   //ID类型
//                "createTime" -> BSONDateTime(DateTime.now.getMillis),//时间类型
//                "clientVersion" -> "1.0.0",       //字符串类型
//                "channel" -> "000255",
//                "state" -> BSONDocument(          //state是个内嵌文档
//                "name" -> "Libai1",
//                "gold" -> 1000                  //整数类型
//      )
//    )
//
//        //插入函数只有一个参数，返回一个Future[LastError]类型的结果，map是用来提取Future变量类型中的数据
//        //具体函数定义请参考API中的BSONCollection  insert
//        playerCollection.insert(newPlayerDoc) map { result =>
//            println("create doc "+result.ok)
//        }
//    }

//    //删
//    def deleteTest() : Unit= {
//
//        //创建一个新的文档，用来指定删除条件
//        val query = BSONDocument("state.name"->"Libai1")
//
//        //第二个参数是可选的，默认为false,具具体函数定义请参考API中的BSONCollection remove
//        playerCollection.remove(query,firstMatchOnly=true) map { result =>
//            println("delete doc "+result.ok)
//        }
//    }
//
//    //改
//    def updateTest() : Unit= {
//
//        //更改条件
//        val selector = BSONDocument("state.name"->"Libai")
//        //更改内容   更新内嵌文档，整数、增加 $inc比$set要高效
//        val updateGold = BSONDocument("$inc"->BSONDocument("state.gold"->500))
//        //更新指定字段内容，若无“$set”，则代表将原文档替换、替换、替换(重要的事情要说三变)成此文档，这个地方要加以小心，替换的话，新文档没有的字段都会删掉
//        val updateVersion = BSONDocument("$set"->BSONDocument("clientVersion"->"1.2.0"))
//
//        //upsert的意思是“若无则创建”，multid的意思是“批量”，具具体函数定义请参考API中的BSONCollection update
//        //playerCollection.update(selector,updateGold,upsert=true,multi=false) map { result =>
//        //  println("update doc "+result.ok)
//        //}
//
//        playerCollection.update(selector,updateVersion,upsert=true,multi=false) map { result =>
//            println("update doc "+result.ok)
//        }
//    }
//
//    //查
//    def findTest() : Unit= {
//
////    val selector = BSONDocument("state.name"->"Libai")
////
////    //普通查询  将查询的结果转换成一个Future[List[BSONDocument]()]
////    //find有两个参数，第一个参数是条件。第二个参数是可选的，projection(投影或映射)意思是查询结果中只保留哪些字段，不填的话是全部保留
////    //collect[List](n) n是一个可选的整数，代表保留前n条数据，不填是查询结果全保留
////    playerCollection.find(selector).cursor[BSONDocument].collect[List]() map { resultDocList =>
////      println("the data are:")
////      for(doc <- resultDocList){                                            //用for取出Future的值
////        val id = doc.getAs[BSONObjectID]("_id").getOrElse(BSONObjectID)     //获取"_id"
////        val clientVersion = doc.getAs[BSONString]("clientVersion").getOrElse(BSONString("no version")).value
////        val stateDoc = doc.getAs[BSONDocument]("state").getOrElse(BSONDocument()) //获取内嵌文档
////        val name = stateDoc.getAs[BSONString]("name").getOrElse(BSONString("no name")).value //获取内嵌文档内字段的值
////        val gold = stateDoc.getAs[BSONInteger]("gold").getOrElse(BSONInteger(0)).value
////
////        println("_id:"+id+",clientVersion:"+clientVersion+",name:"+name+",gold:"+gold)
////      }
////    }
//
///**   getAs[BSONString](“name”).getOrElse(BSONString(“unknown”)).value
// *     也可以写成
// *     getAs[String](“name”).getOrElse(“unknown”)
// **/    Int Boolean等都类似
//
//        //聚合查询
////    假设有如下数据每个channel下有3个玩家，我想查询每个channel下的玩家gold之和是多少。
////    { "_id" : ..., "createTime" : ..., "clientVersion" : "1.0.0", "channel" : "000255", "state" : { "name" : "Libai1", "gold" : 1000 } }
////    { "_id" : ..., "createTime" : ..., "clientVersion" : "1.0.0", "channel" : "000255", "state" : { "name" : "Libai2", "gold" : 2000 } }
////    { "_id" : ..., "createTime" : ..., "clientVersion" : "1.0.0", "channel" : "000255", "state" : { "name" : "Libai3", "gold" : 3000 } }
////    { "_id" : ..., "createTime" : ..., "clientVersion" : "1.0.0", "channel" : "100120", "state" : { "name" : "Baijuyi1", "gold" : 1500 } }
////    { "_id" : ..., "createTime" : ..., "clientVersion" : "1.0.0", "channel" : "100120", "state" : { "name" : "Baijuyi2", "gold" : 2500 } }
////    { "_id" : ..., "createTime" : ..., "clientVersion" : "1.0.0", "channel" : "100120", "state" : { "name" : "Baijuyi3", "gold" : 3500 } }
////    { "_id" : ..., "createTime" : ..., "clientVersion" : "1.0.0", "channel" : "000066", "state" : { "name" : "Wanganshi1", "gold" : 1200 } }
////    { "_id" : ..., "createTime" : ..., "clientVersion" : "1.0.0", "channel" : "000066", "state" : { "name" : "Wanganshi2", "gold" : 2200 } }
////    { "_id" : ..., "createTime" : ..., "clientVersion" : "1.0.0", "channel" : "000066", "state" : { "name" : "Wanganshi3", "gold" : 3200 } }
//
//        //$group,$sort,$limit,$sum都是聚合关键字，_id是ID关键字 这些都不能变。amount是自己取得名字，将后面$sum的结果赋值给这个变量
//        //注意下面那些地方用到了$,$,$(重要的事情要说三遍)符号
//        val commondDoc = BSONDocument(
//                "aggregate" -> "players",
//                "pipeline"  -> BSONArray(
//                BSONDocument("$group" -> BSONDocument("_id"->"$channel","amount"->BSONDocument("$sum"->"$state.gold"))),  //分组
//        BSONDocument("$sort"  -> BSONDocument("amount" -> -1)), //排序
//        BSONDocument("$limit" -> 3) //选取特定数量的返回值
//      )
//    )
//
//        val resultFuture = db.command(RawCommand(commondDoc))//默认返回 Future[BSONDocument]
//
//        //BSONDocument里存放一个以“result”(就是result这个名字，不是我自己起的)命名BSONArray来保存聚合查询的数据
//        for(result <- resultFuture){
//            val rankArray = result.getAs[BSONArray]("result").getOrElse(BSONArray())
//            for(i<-0 until rankArray.length){
//                val record = rankArray.getAs[BSONDocument](i).getOrElse(BSONDocument())
//                val channel = record.getAs[String]("_id").getOrElse("no channel")
//                val amount = record.getAs[Int]("amount").getOrElse(0) //这个"amount"变量名，要和创建doc时，分组中的那个”amount“保持一致
//                println("channel:"+channel+",amount:"+amount)
//            }
//        }
//    }
//
//
//
//
//
//
//    object RMongoStudy{
//        def main (args: Array[String]) {
//        val mongo = new RMongoStudy()
////    mongo.createTest()
////    mongo.deleteTest()
////    mongo.updateTest()
//        mongo.findTest()
//        }
//        }
//    ))
//
