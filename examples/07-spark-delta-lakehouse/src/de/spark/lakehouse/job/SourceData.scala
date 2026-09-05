package de.spark.lakehouse.job

import de.common.domain.Order
import de.common.gen.DataGenerator
import de.spark.lakehouse.core.BronzeRecords
import de.spark.lakehouse.core.BronzeRecords.{OrderRow, PaymentRow, ShipmentRow}

/**
 * Stands in for the operational systems a real lakehouse ingests from.
 *
 * In production the bronze layer is fed by a change-data-capture stream, a nightly export or an Apache Kafka topic. To
 * keep this example self-contained it is fed by the shared, seeded generator instead, so every run produces exactly the
 * same orders and the numbers in the README stay true.
 */
object SourceData {

  /** The three raw feeds that land in bronze, plus the deliberately imperfect rows that make cleaning worth doing. */
  final case class Batch(
      orders: Seq[OrderRow],
      payments: Seq[PaymentRow],
      shipments: Seq[ShipmentRow]
  )

  /**
   * Generates one batch of raw events.
   *
   * Two flaws are injected on purpose, because a bronze layer that is already clean teaches nothing:
   *
   *   - every twentieth order is landed twice with a later ingestion timestamp, imitating a producer that retried after
   *     a network timeout and delivered the same record again;
   *   - a small number of orders arrive with an empty customer identifier, imitating a partially filled form.
   *
   * The silver transformations are what remove both.
   *
   * @param orderCount
   *   how many distinct orders to generate
   * @param ingestedAtEpochMillis
   *   the moment this batch is considered to have landed
   */
  def generate(orderCount: Int, ingestedAtEpochMillis: Long): Batch = {
    val generator = new DataGenerator()
    val orders    = generator.orders(orderCount)

    val orderRows = orders.zipWithIndex.flatMap { case (order, index) =>
      val landed = BronzeRecords.orderRow(damage(order, index), ingestedAtEpochMillis)
      if (index % 20 == 0) Seq(landed, landed.copy(ingestedAtEpochMillis = ingestedAtEpochMillis + 1000L))
      else Seq(landed)
    }

    val paymentRows  = orders.map(order => BronzeRecords.paymentRow(generator.paymentFor(order), ingestedAtEpochMillis))
    val shipmentRows =
      orders.flatMap(order => generator.shipmentsFor(order).map(BronzeRecords.shipmentRow(_, ingestedAtEpochMillis)))

    Batch(orderRows, paymentRows, shipmentRows)
  }

  /**
   * Corrections that arrive after the fact: a handful of the original orders come back with a different country,
   * because the customer moved or the address was fixed in the shop's back office.
   *
   * Feeding these through `MERGE INTO` is what the slowly changing dimension part of the example demonstrates.
   */
  def corrections(orderCount: Int, ingestedAtEpochMillis: Long): Seq[OrderRow] = {
    val generator = new DataGenerator()
    generator
      .orders(orderCount)
      .take(10)
      .map(order => BronzeRecords.orderRow(order.copy(country = "NL"), ingestedAtEpochMillis))
  }

  /** Empties the customer identifier of every fiftieth order so the cleaning step has something to reject. */
  private def damage(order: Order, index: Int): Order =
    if (index % 50 == 0) order.copy(customerId = de.common.domain.CustomerId("")) else order
}
