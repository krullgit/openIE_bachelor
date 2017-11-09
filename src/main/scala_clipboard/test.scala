package com.sksamuel.elastic4s.samples


import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.mappings.FieldType.NestedType
import org.scalatest.{FreeSpec, Matchers}
import com.sksamuel.elastic4s.testkit.ElasticSugar
import org.elasticsearch.common.text.Text
import org.elasticsearch.search.highlight.HighlightField

class NestedQueryTest extends FreeSpec with Matchers with ElasticSugar {

  client.execute {
    create index "nested" mappings {
      mapping("show") as {
        field("actor") typed NestedType
      }
    }
  }.await

  client.execute(
    index into "nested/show" fields (
      field("name") -> "game of thrones",
      field("actor") -> Seq(
        Map("name" -> "peter dinklage", "birthplace" -> "Morristown"),
        Map("name" -> "pedro pascal", "birthplace" -> "Santiago")
      )
    )
  ).await

  refresh("nested")
  blockUntilCount(1, "nested")

  "nested object" - {
    "should be searchable by nested field" in {
      val resp1 = client.execute {
        search in "nested/show" query nestedQuery("actor").query(termQuery("actor.name" -> "dinklage"))
      }.await
      resp1.totalHits shouldEqual 1

      val resp2 = client.execute {
        search in "nested/show" query nestedQuery("actor").query(termQuery("actor.name" -> "simon"))
      }.await
      resp2.totalHits shouldEqual 0
    }
  }

  "nested object" - {
    "should be presented in highlighting" in {
      val resp1 = client.execute {
        search in "nested/show" query nestedQuery("actor").query(termQuery("actor.name" -> "dinklage")).inner {
          innerHits("actor").highlighting(highlight.field("actor.name").matchedFields("actor.name").fragmentSize(20))
        }
      }.await
      resp1.totalHits shouldEqual 1
      val maybeHits = resp1.hits(0).innerHits.get("actor")
      maybeHits.isDefined shouldBe true
      maybeHits.get.getTotalHits shouldEqual 1
      val fields = maybeHits.get.getAt(0).highlightFields
      fields.containsKey("actor.name") shouldBe true
      val fragments = fields.get("actor.name").fragments()
      fragments.length shouldBe 1
      fragments(0).string shouldBe "peter <em>dinklage</em>"
    }
  }

  "nested object" - {
    "should have correct inner hit source" in {
      val resp1 = client.execute {
        search in "nested/show" query nestedQuery("actor").query(termQuery("actor.name" -> "dinklage")).inner {
          innerHits("actor").sourceExclude("birthplace")
        }
      }.await
      resp1.totalHits shouldEqual 1
      val maybeInnerHits = resp1.hits(0).innerHits.get("actor")
      maybeInnerHits.isDefined shouldBe true
      maybeInnerHits.get.getTotalHits shouldEqual 1
      maybeInnerHits.get.getAt(0).sourceAsMap.containsKey("birthplace") shouldBe false
      maybeInnerHits.get.getAt(0).sourceAsMap.containsKey("name") shouldBe true
    }
  }
}
