package de.hugegraph.fraud

/**
 * The graph schema this example creates in HugeGraph.
 *
 * Unlike many graph databases, HugeGraph insists on a declared schema before any data may be written. That is closer to
 * a relational database than to a schema-free document store, and it buys the same things: typed properties, cheap
 * validation, and indexes the query planner can use.
 *
 * A schema is built from four kinds of element, and they must be created in this order because each one refers to the
 * previous:
 *
 *   1. **property keys** - the typed fields a vertex or edge may carry, for example `country: TEXT`.
 *   2. **vertex labels** - the kinds of entity, listing which property keys they use.
 *   3. **edge labels** - the kinds of relationship, naming the vertex label at each end.
 *   4. **index labels** - the indexes. Without one, a lookup such as "customers in Poland" would have to scan every
 *      vertex.
 *
 * These case classes mirror the JSON bodies of the HugeGraph schema endpoints one field at a time; [[Payloads]] does
 * the rendering. Splitting it this way means the shape of the schema can be unit-tested without a running server.
 */
object FraudSchema {

  /** The property types HugeGraph offers that this example uses. */
  enum DataType(val wireName: String) {
    case Text extends DataType("TEXT")
    case Long extends DataType("LONG")
  }

  /** A typed field. `SINGLE` cardinality means one value per key, which is all this domain needs. */
  final case class PropertyKey(name: String, dataType: DataType)

  /**
   * A kind of vertex.
   *
   * @param idStrategy
   *   fixed to `CUSTOMIZE_STRING` for every label here: the loader supplies the identifier (`cust-0042`, `card-7`)
   *   rather than letting HugeGraph generate one. Stable identifiers make the load idempotent and make the Gremlin
   *   examples in the README readable.
   */
  final case class VertexLabel(name: String, properties: List[String]) {
    val idStrategy: String = "CUSTOMIZE_STRING"
  }

  /**
   * A kind of edge, from one vertex label to another.
   *
   * @param frequency
   *   `SINGLE` means at most one edge of this label between a given pair of vertices. An order is paid with a card
   *   once, so a second load of the same data updates the edge instead of adding a parallel one.
   */
  final case class EdgeLabel(name: String, sourceLabel: String, targetLabel: String, properties: List[String]) {
    val frequency: String = "SINGLE"
  }

  /**
   * An index over one vertex or edge property.
   *
   * `SECONDARY` is HugeGraph's equal-match index: it answers `has('country', 'PL')` but not a range query. That is the
   * right kind here because every indexed field in this schema is a category, not a measure.
   */
  final case class IndexLabel(name: String, baseType: String, baseValue: String, fields: List[String]) {
    val indexType: String = "SECONDARY"
  }

  final case class Schema(
      propertyKeys: List[PropertyKey],
      vertexLabels: List[VertexLabel],
      edgeLabels: List[EdgeLabel],
      indexLabels: List[IndexLabel]
  )

  /** Vertex label names, kept in one place so queries and the loader cannot drift apart. */
  object Vertices {
    val Customer = "customer"
    val Order    = "order"
    val Card     = "card"
    val Device   = "device"
    val Address  = "address"

    /** The three labels that a fraud ring is discovered *through*: the things two accounts can share. */
    val sharedArtefacts: List[String] = List(Card, Device, Address)
  }

  /** Edge label names. */
  object Edges {
    val Placed            = "placed"
    val PaidWith          = "paid_with"
    val PlacedFrom        = "placed_from"
    val ShipsTo           = "ships_to"
    val fromOrder         = List(PaidWith, PlacedFrom, ShipsTo)
    val all: List[String] = Placed :: fromOrder
  }

  /** Property key names. */
  object Properties {
    val CustomerId  = "customer_id"
    val OrderId     = "order_id"
    val CardId      = "card_id"
    val DeviceId    = "device_id"
    val AddressId   = "address_id"
    val Country     = "country"
    val City        = "city"
    val Issuer      = "issuer"
    val Platform    = "platform"
    val TotalCents  = "total_cents"
    val AmountCents = "amount_cents"
    val PlacedAt    = "placed_at"
  }

  /**
   * The schema of the online-shop fraud graph.
   *
   * Five kinds of vertex and four kinds of edge. An order is the hub: it points at the customer who placed it and at
   * the three artefacts that identify *how* it was placed. Two customers who never met can therefore still be two hops
   * apart through a card, a device or an address they share.
   */
  val shop: Schema = {
    import DataType.*
    import Properties.*
    import Vertices.*
    import Edges.*

    Schema(
      propertyKeys = List(
        PropertyKey(CustomerId, Text),
        PropertyKey(OrderId, Text),
        PropertyKey(CardId, Text),
        PropertyKey(DeviceId, Text),
        PropertyKey(AddressId, Text),
        PropertyKey(Country, Text),
        PropertyKey(City, Text),
        PropertyKey(Issuer, Text),
        PropertyKey(Platform, Text),
        PropertyKey(TotalCents, Long),
        PropertyKey(AmountCents, Long),
        PropertyKey(PlacedAt, Long)
      ),
      vertexLabels = List(
        VertexLabel(Customer, List(CustomerId, Country)),
        VertexLabel(Order, List(OrderId, TotalCents, PlacedAt, Country)),
        VertexLabel(Card, List(CardId, Issuer)),
        VertexLabel(Device, List(DeviceId, Platform)),
        VertexLabel(Address, List(AddressId, City, Country))
      ),
      edgeLabels = List(
        EdgeLabel(Placed, Customer, Order, List(PlacedAt)),
        EdgeLabel(PaidWith, Order, Card, List(AmountCents)),
        EdgeLabel(PlacedFrom, Order, Device, Nil),
        EdgeLabel(ShipsTo, Order, Address, Nil)
      ),
      indexLabels = List(
        IndexLabel("customerByCountry", "VERTEX_LABEL", Customer, List(Country)),
        IndexLabel("orderByCountry", "VERTEX_LABEL", Order, List(Country)),
        IndexLabel("addressByCity", "VERTEX_LABEL", Address, List(City)),
        IndexLabel("cardByIssuer", "VERTEX_LABEL", Card, List(Issuer))
      )
    )
  }
}
