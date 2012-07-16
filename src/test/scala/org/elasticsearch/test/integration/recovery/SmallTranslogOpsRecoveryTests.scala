package org.elasticsearch.test.integration.recovery

import org.elasticsearch.common.settings._

class SmallTranslogOpsRecoveryTests extends SimpleRecoveryTests {

  protected override def recoverySettings = 
    ImmutableSettings.settingsBuilder.put("shard.recovery.translog_ops", 1).build
}
