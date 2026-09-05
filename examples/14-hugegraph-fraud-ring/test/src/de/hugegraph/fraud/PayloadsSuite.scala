package de.hugegraph.fraud

import de.hugegraph.fraud.FraudSchema.*
import de.hugegraph.fraud.GremlinQueries.GremlinQuery
import de.hugegraph.fraud.PropertyGraph.{Edge, Value, Vertex}

/**
 * Tests for the JSON bodies sent to HugeGraph.
 *
 * The expectations are written as JSON text and parsed, rather than as `ujson.Obj` builders, so that a reader can see
 * exactly what goes over the wire.
 */
class PayloadsSuite extends munit.FunSuite {

  private def json(text: String): ujson.Value = ujson.read(text)

  test("a property key carries its HugeGraph data type name") {
    assertEquals(
      Payloads.propertyKey(PropertyKey("total_cents", DataType.Long)),
      json("""{"name":"total_cents","data_type":"LONG","cardinality":"SINGLE"}""")
    )
  }

  test("a vertex label declares customised string identifiers and a label index") {
    assertEquals(
      Payloads.vertexLabel(VertexLabel("customer", List("customer_id", "country"))),
      json("""
        {
          "name": "customer",
          "id_strategy": "CUSTOMIZE_STRING",
          "properties": ["customer_id", "country"],
          "nullable_keys": [],
          "enable_label_index": true
        }
      """)
    )
  }

  test("an edge label names the vertex label at each end") {
    assertEquals(
      Payloads.edgeLabel(EdgeLabel("paid_with", "order", "card", List("amount_cents"))),
      json("""
        {
          "name": "paid_with",
          "source_label": "order",
          "target_label": "card",
          "frequency": "SINGLE",
          "properties": ["amount_cents"],
          "sort_keys": [],
          "nullable_keys": [],
          "enable_label_index": true
        }
      """)
    )
  }

  test("an index label is a secondary index over one field") {
    assertEquals(
      Payloads.indexLabel(IndexLabel("customerByCountry", "VERTEX_LABEL", "customer", List("country"))),
      json("""
        {
          "name": "customerByCountry",
          "base_type": "VERTEX_LABEL",
          "base_value": "customer",
          "index_type": "SECONDARY",
          "fields": ["country"]
        }
      """)
    )
  }

  test("a vertex payload keeps text and numeric properties apart") {
    val vertex =
      Vertex("order", "order-1", Map("order_id" -> Value.Text("order-1"), "total_cents" -> Value.Number(4200L)))

    assertEquals(
      Payloads.vertex(vertex),
      json("""{"id":"order-1","label":"order","properties":{"order_id":"order-1","total_cents":4200}}""")
    )
  }

  test("an edge payload carries both endpoint identifiers and both endpoint labels") {
    val edge = Edge("paid_with", "order-1", "order", "card-7", "card", Map("amount_cents" -> Value.Number(4200L)))

    assertEquals(
      Payloads.edge(edge),
      json("""
        {
          "label": "paid_with",
          "outV": "order-1",
          "outVLabel": "order",
          "inV": "card-7",
          "inVLabel": "card",
          "properties": {"amount_cents": 4200}
        }
      """)
    )
  }

  test("a gremlin request aliases the traversal source of the named graph") {
    assertEquals(
      Payloads.gremlinRequest(GremlinQuery("g.V(source)", Map("source" -> "cust-1")), graph = "hugegraph"),
      json("""
        {
          "gremlin": "g.V(source)",
          "bindings": {"source": "cust-1"},
          "language": "gremlin-groovy",
          "aliases": {"graph": "hugegraph", "g": "__g_hugegraph"}
        }
      """)
    )
  }

  test("every property used by a label in the shop schema has a declared property key") {
    val declared = FraudSchema.shop.propertyKeys.map(_.name).toSet
    val used     =
      FraudSchema.shop.vertexLabels.flatMap(_.properties) ++
        FraudSchema.shop.edgeLabels.flatMap(_.properties) ++
        FraudSchema.shop.indexLabels.flatMap(_.fields)

    assertEquals(used.toSet.diff(declared), Set.empty[String])
  }

  test("every edge label in the shop schema connects two declared vertex labels") {
    val vertexLabels = FraudSchema.shop.vertexLabels.map(_.name).toSet
    val endpoints    = FraudSchema.shop.edgeLabels.flatMap(label => List(label.sourceLabel, label.targetLabel))

    assertEquals(endpoints.toSet.diff(vertexLabels), Set.empty[String])
  }
}
