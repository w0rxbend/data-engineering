package de.flink.cep.sla.job

import de.flink.cep.sla.core.ShipmentRecords
import org.apache.flink.util.OutputTag

/**
 * The extra outlets of the pattern-matching operator.
 *
 * An Apache Flink operator has one main output, but it may also push records into any number of *side outputs*. A side
 * output is identified by an `OutputTag`: the operator writes to the tag, and the pipeline reads the matching stream
 * back with `getSideOutput`. Two tags are equal when their identifier strings are equal, so the tag used for writing
 * and the tag used for reading must be the same value -- which is why they live here instead of being created twice.
 *
 * Breaches leave through the side output because they are produced by a *different* callback than ordinary matches:
 * `processTimedOutMatch` instead of `processMatch`. Only the main output has a return channel through the `Collector`,
 * so a timed-out match has no other way out.
 */
object SlaOutputTags {

  /** Alerts about a promise that was broken. */
  val breaches: OutputTag[ShipmentRecords.Alert] =
    new OutputTag[ShipmentRecords.Alert]("sla-breaches", ShipmentRecords.alertTypeInformation)
}
