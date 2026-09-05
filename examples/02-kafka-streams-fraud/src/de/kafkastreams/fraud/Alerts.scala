package de.kafkastreams.fraud

import de.common.domain.{CustomerId, Money, OrderId, PaymentStatus}

/**
 * The vocabulary this example adds on top of the shared online-shop domain (`de.common.domain`). Everything here is a
 * plain immutable value: no Kafka type appears in this file, so all of it can be unit tested on its own.
 */

/**
 * One order together with the payment attempt that belongs to it.
 *
 * The `orders` topic and the `payments` topic each carry only half of the picture. An order knows *who* bought
 * (`customerId`) but not whether the card was accepted; a payment knows the outcome but is keyed by order, not by
 * customer. Joining the two gives a record that finally answers the question this example cares about: "did *this
 * customer* just get declined?".
 */
final case class PaidOrder(
    orderId: OrderId,
    customerId: CustomerId,
    country: String,
    amount: Money,
    status: PaymentStatus,
    occurredAtEpochMillis: Long
) {
  def isDeclined: Boolean = status == PaymentStatus.Declined
}

/**
 * Running total of declined payments for one customer inside one time window.
 *
 * Kafka Streams keeps one of these per (customer, window) pair in a state store, updating it every time another
 * declined payment arrives.
 */
final case class DeclineTally(count: Int, totalCents: Long) {
  def add(payment: PaidOrder): DeclineTally =
    DeclineTally(count + 1, totalCents + payment.amount.cents)
}

object DeclineTally {

  /** The value a window starts from before the first declined payment lands. */
  val empty: DeclineTally = DeclineTally(0, 0L)
}

/**
 * How suspicious a customer already was before this window.
 *
 * This arrives on its own compacted topic and is read as a table rather than as a stream: only the newest value per
 * customer matters.
 */
final case class CustomerRisk(customerId: CustomerId, tier: String)

object CustomerRisk {

  /** Used when the customer has no entry in the risk table at all. */
  val unknownTier: String = "unknown"
}

/** The message this example publishes to the `fraud-alerts` topic. */
final case class FraudAlert(
    customerId: CustomerId,
    declinedCount: Int,
    totalDeclinedCents: Long,
    windowStartEpochMillis: Long,
    windowEndEpochMillis: Long,
    riskTier: String
) {
  def withRiskTier(tier: String): FraudAlert = copy(riskTier = tier)
}

/**
 * The business rule, expressed without any streaming machinery.
 *
 * "Card testing" is a fraud pattern where somebody with a list of stolen card numbers fires many small orders in quick
 * succession to find out which cards still work. Most of those attempts are declined, so a burst of declines from a
 * single customer inside a short window is the signal to alert on.
 */
object FraudRules {

  /**
   * Turns one finished window into an alert, or into nothing when the window stayed below the threshold.
   *
   * @param threshold
   *   how many declined payments inside the window are needed before the customer is reported
   */
  def alertFor(
      customerId: CustomerId,
      tally: DeclineTally,
      windowStartEpochMillis: Long,
      windowEndEpochMillis: Long,
      threshold: Int
  ): Option[FraudAlert] =
    if (tally.count >= threshold) {
      Some(
        FraudAlert(
          customerId = customerId,
          declinedCount = tally.count,
          totalDeclinedCents = tally.totalCents,
          windowStartEpochMillis = windowStartEpochMillis,
          windowEndEpochMillis = windowEndEpochMillis,
          riskTier = CustomerRisk.unknownTier
        )
      )
    } else {
      None
    }
}
