package org.elasticsearch.test.integration.recovery

import org.elasticsearch.common.settings.ImmutableSettings._

class SmallTranslogSizeRecoveryTests extends SimpleRecoveryTests {

  protected override def recoverySettings =
    settingsBuilder.put("shard.recovery.translog_size", "3b").build
}
