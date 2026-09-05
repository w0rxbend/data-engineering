package de.kafka.eos

import de.common.gen.DataGenerator
import de.common.json.Codecs
import munit.FunSuite

/**
 * Checks that this example can read back exactly what the shared `Codecs` object writes. Every example in the
 * repository shares that layout, so a mismatch here would be a mismatch with all of them.
 */
final class OrderJsonTest extends FunSuite {

  test("decodes an order written by the shared codec back into the same value") {
    val original = new DataGenerator(seed = 7L).nextOrder()
    assertEquals(OrderJson.decode(Codecs.order(original)), Right(original))
  }

  test("reports text that is not JSON at all") {
    assert(OrderJson.decode("not json").left.exists(_.startsWith("not valid JSON")))
  }

  test("reports JSON that is missing a field the order needs") {
    assert(OrderJson.decode("""{"id":"order-1"}""").left.exists(_.startsWith("does not match the order layout")))
  }
}
