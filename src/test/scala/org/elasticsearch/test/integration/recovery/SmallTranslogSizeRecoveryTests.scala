package org.elasticsearch.test.integration.recovery

import org.elasticsearch.common.settings._

class SmallTranslogSizeRecoveryTests extends SimpleRecoveryTests {

  protected override def recoverySettings =
    ImmutableSettings.settingsBuilder.put("shard.recovery.translog_size", "3b").build
}
