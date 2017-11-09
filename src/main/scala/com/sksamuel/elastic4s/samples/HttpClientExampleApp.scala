package com.sksamuel.elastic4s.samples


import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.search.{SearchHit, SearchResponse}
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.samples.HttpClientExampleApp.{typeList, typeQuery}

import scala.collection.immutable
import scala.util.control.Exception


object HttpClientExampleApp extends App {

  // METHODS

  // just prints the text field from the results
  def printHits(x: List[SearchHit]) {
    println("#results: " + x.size)
    x.foreach(y => println(y.sourceField(("text")).asInstanceOf[Map[Any, Any]].get("string") match {
      case Some(i) => println(i)
      case _ => ""
    }))
  }

  // prints a list and asks to choose some
  def chooseSomething(list: List[String], topic: String): List[String] = {
    println(">ENTITY1< >TRIGGER< >ENTITY2<\n")
    val listClean = list.filter(i => !i.equals("")).distinct.filter(i => !i.equals("trigger"))
    println(s"///// $topic /////\n")
    for (i <- listClean.indices) {
      println(s"$i ${listClean(i)}")
    }
    println("")
    val y = scala.io.StdIn.readLine(s"Please choose $topic (e.g.: 1,2,3): ")
    if (y.equals("")) {
      val temp = List.range(0, listClean.size).map(x => listClean(x))
      println(s"Chosen $topic: " + temp + "\n")
      temp
    } else {
      val temp = y.split(",").toList.map(x => x.toInt).map(x => listClean(x))
      println(s"Chosen $topic: " + temp + "\n")
      temp
    }
  }

  // to option (sometimes useful)
  def toOption(value: Any): Option[Any] = Option(value)

  // can flatten nested Lists
  def flatten(ls: List[Any]): List[Any] = ls flatMap {
    case i: List[_] => flatten(i)
    case e => List(e)
  }


  def getValueListofLists(response: List[SearchHit], path: String): List[List[String]] = {
    val temp = for (j <- response.indices) yield {
      getField(response(j), path)
    }
    temp.toList //.filter(i => !i.equals("")).distinct
  }

  // returns a list with strings from the response
  def getValueList(response: List[SearchHit], path: String): List[String] = {
    val temp = for (j <- response.indices) yield {
      getField(response(j), path)
    }
    temp.flatten.toList //.filter(i => !i.equals("")).distinct
  }

  def getField(searchhit: SearchHit, path: String): List[String] = {
    val pathSplit:List[String] = path.split("\\.").toList
    val sourceField = pathSplit(0)
    try {
      searchhit.sourceField(sourceField)
    } catch {
      case e: Exception => return List("")
    }
    val listBuilder = List.newBuilder[String]
    val y: Any = searchhit.sourceField(sourceField)

    downThePath(1, y)

    def downThePath(count: Int, innerElement: Any) {
      innerElement.asInstanceOf[Map[Any, Any]] match {
        case i: Map[Any, Any] => i.get(pathSplit(count)) match {
          case Some(j) => j match {
            case k: List[Any] =>
              if (k.nonEmpty) {
                k.foreach(downThePath(count + 1, _))
              }
            case l: Map[Any, Any] => downThePath(count + 1, l)
            case i: String => listBuilder += i
            case _ => println("Error: no match")
          }
          case _ => println("Error: failed get")
        }
        case _ => println("Error: this is not a Map")
      }
    }
    listBuilder.result()
  }

  def execute(Query: String = "{}", SourceInclude: List[String]) = {
    client.execute { // get all conceptMentions (which has the relationMentions)
      search("rss" / "rss3").rawQuery(Query) sourceInclude SourceInclude size 1000 //.*Hamburg.*Berlin.*
    }.await.hits.hits.toList
  }

  def printSuggestions(roleList: List[List[String]], typeList: List[List[String]], valueList: List[List[String]], chosenEntities1:List[String],chosenEntities2:List[String] ) ={

    val verbs = List("trigger")
    chosenEntities1.foreach(x => print(""+x+" "))
    print("// ")
    verbs.foreach(x => print(""+x+" "))
    print("// ")
    chosenEntities2.foreach(x => print(""+x+" "))
    print("\n\n")

    val listbuilder = List.newBuilder[String]

    for (counterTypeLists <- typeList.indices) {
      listbuilder.clear()
      for (counterType <- typeList(counterTypeLists).indices ) {
        if(chosenEntities1.contains(typeList(counterTypeLists)(counterType))){
          listbuilder += valueList(counterTypeLists)(counterType)
        }
      }
      listbuilder += " // "
      for (counterType <- typeList(counterTypeLists).indices ) {
        if(verbs.contains(typeList(counterTypeLists)(counterType))){
          listbuilder += valueList(counterTypeLists)(counterType)
        }
      }
      listbuilder += " // "

      for (counterType <- typeList(counterTypeLists).indices ) {
        if(chosenEntities2.contains(typeList(counterTypeLists)(counterType))){
          listbuilder += valueList(counterTypeLists)(counterType)
        }
      }
      listbuilder.result().foreach(x => print(""+x+" "))
      println("")
    }
  }

  // MAIN
  val client = HttpClient(ElasticsearchClientUri("localhost", 9200)) // new client

  val relSourceInclude = List("relationMentions.array.name")
  val relResp = execute(SourceInclude = relSourceInclude) // get all conceptMentions (which has the relationMentions)
  var relList = getValueList(relResp, "relationMentions.array.name") // get all relationMentions

  val chosenRel: List[String] = chooseSomething(relList, "triggers") // choose some relationMentions
  val chosenRelAsRegex = chosenRel.mkString("|")

  val typeQuery = "{\"regexp\":{\"relationMentions.array.name.keyword\":{\"value\":\"" + chosenRelAsRegex + "\",\"boost\":1.2}}    }"
  val typeSourceInclude = List("relationMentions.array.args.array.conceptMention.type")
  val typeResp = execute(typeQuery, typeSourceInclude) // get all conceptMentions (which has the relationMentions)
  val typeList = getValueList(typeResp, "relationMentions.array.args.array.conceptMention.type") // get all relationMentions

  val chosenEntities1 = chooseSomething(typeList, "entity 1") // choose some Entities
  val chosenEntities2 = chooseSomething(typeList, "entity 2") // choose some Entities
  val chosenEntities1AsRegex = chosenEntities1.mkString("|")
  val chosenEntities2AsRegex = chosenEntities2.mkString("|")

  val iteration1Query = "{\"bool\":{\"must\":[{\"regexp\":{\"relationMentions.array.name.keyword\":\"" + chosenRelAsRegex + "\"}},{\"regexp\":{\"conceptMentions.array.type.keyword\":\"" + chosenEntities1AsRegex + "\"}},{\"regexp\":{\"conceptMentions.array.type.keyword\":\"" + chosenEntities2AsRegex + "\"}}]}}"
  val iteration1SourceInclude = List("text","relationMentions.array.args.array.role","relationMentions.array.args.array.conceptMention.type","relationMentions.array.args.array.conceptMention.normalizedValue.string")
  val iteration1Resp = execute(iteration1Query, iteration1SourceInclude) // get all conceptMentions (which has the relationMentions)

  val iteration1RoleList = getValueListofLists(iteration1Resp, "relationMentions.array.args.array.role")
  val iteration1TypeList = getValueListofLists(iteration1Resp, "relationMentions.array.args.array.conceptMention.type")
  val iteration1ValueList = getValueListofLists(iteration1Resp, "relationMentions.array.args.array.conceptMention.normalizedValue.string")

  printSuggestions(iteration1RoleList,iteration1TypeList,iteration1ValueList,chosenEntities1, chosenEntities2)

  client.close()
}
