package org.elasticsearch.test.integration.recovery

import org.elasticsearch.common.settings._

class SmallFileChunkSizeRecoveryTests extends SimpleRecoveryTests {

  protected override def recoverySettings = 
    ImmutableSettings.settingsBuilder.put("shard.recovery.file_chunk_size", "3b").build
}
