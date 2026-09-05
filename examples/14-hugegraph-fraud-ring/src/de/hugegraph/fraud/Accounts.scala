package de.hugegraph.fraud

import de.common.domain.CustomerId

/**
 * Who paid with which card, from which device, to which address.
 *
 * The shared domain model in `common/` describes orders, payments and shipments, but it does not describe the three
 * artefacts a fraud analyst actually correlates on. This file adds them, deterministically: the same customer
 * identifier always yields the same card, device and address, so the whole example is reproducible from a seed.
 *
 * By default every customer gets their own private artefacts, which is what an honest population looks like. A
 * [[RingPlan]] then overrides that for a chosen handful of customers, making them share something. Planting the rings
 * explicitly, instead of hoping random data collides, is what lets the tests state an exact expected answer.
 */
object Accounts {

  /** The artefacts one customer uses. */
  final case class Account(customerId: String, cardId: String, deviceId: String, addressId: String)

  /**
   * A deliberately planted fraud ring.
   *
   * @param name
   *   a human label used only in the printed report.
   * @param members
   *   the customer identifiers that belong to the ring.
   * @param sharedCardId
   *   if set, every member pays with this one card.
   * @param sharedDeviceId
   *   if set, every member orders from this one device.
   * @param sharedAddressId
   *   if set, every member ships to this one address.
   */
  final case class RingPlan(
      name: String,
      members: List[String],
      sharedCardId: Option[String] = None,
      sharedDeviceId: Option[String] = None,
      sharedAddressId: Option[String] = None
  )

  private val issuers   = Vector("visa", "mastercard", "amex")
  private val platforms = Vector("android", "ios", "web")
  private val cities    = Vector("Berlin", "Warsaw", "Kyiv", "Lyon", "Madrid")

  /** The trailing part of `cust-0042`, used to name that customer's private artefacts. */
  private def suffix(customerId: String): String = customerId.split('-').last

  /**
   * A stable, non-negative index into a vocabulary.
   *
   * `String.hashCode` is used rather than a random number generator so that the value depends only on the identifier:
   * loading the graph twice, or in a different order, produces byte-identical vertices.
   */
  private def pick[A](vocabulary: Vector[A], key: String): A =
    vocabulary(math.floorMod(key.hashCode, vocabulary.size))

  def issuerOf(cardId: String): String     = pick(issuers, cardId)
  def platformOf(deviceId: String): String = pick(platforms, deviceId)
  def cityOf(addressId: String): String    = pick(cities, addressId)

  /** The honest baseline: private card, device and address per customer. */
  def privateAccount(customerId: String): Account = {
    val key = suffix(customerId)
    Account(customerId, s"card-$key", s"device-$key", s"address-$key")
  }

  /**
   * Assigns artefacts to every customer, then rewrites the members of each ring plan so that they share what the plan
   * says they share.
   *
   * A customer named by a plan but absent from `customerIds` is ignored: the plan describes an intent, and it stays
   * correct even if a smaller batch of orders happens not to contain that customer.
   */
  def assign(customerIds: Iterable[CustomerId], plans: List[RingPlan]): Map[String, Account] = {
    val baseline = customerIds.map(_.value).toList.distinct.map(id => id -> privateAccount(id)).toMap

    plans.foldLeft(baseline) { (accounts, plan) =>
      plan.members.foldLeft(accounts) { (updated, member) =>
        updated.get(member) match {
          case None          => updated
          case Some(account) =>
            updated.updated(
              member,
              account.copy(
                cardId = plan.sharedCardId.getOrElse(account.cardId),
                deviceId = plan.sharedDeviceId.getOrElse(account.deviceId),
                addressId = plan.sharedAddressId.getOrElse(account.addressId)
              )
            )
        }
      }
    }
  }
}
