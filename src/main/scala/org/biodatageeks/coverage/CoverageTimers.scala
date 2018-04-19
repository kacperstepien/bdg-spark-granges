package org.biodatageeks.coverage

import org.bdgenomics.utils.instrumentation.Metrics

/**
 * Created by mwiewior on 25.06.16.
 */
object CoverageTimers extends Metrics{

  val CovTableWriterTimer = timer("Coverage table writer timer")
  val BaseCoverageTimer = timer("Base coverage method timer")
  val LoadSamplesTimer = timer("Load samples timer")

}
