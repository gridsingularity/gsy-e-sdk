Feature: Batch Commands Tests

Scenario: Batch Commands sent successfully
   Given redis container is started
   And gsy-e is started in paused mode using setup strategy_tests.external_devices (-t 60s -s 60m -d 12h --slot-length-realtime 2s)
   When the gsy-e-sdk is connecting to gsy-e with test_aggregator_batch_commands
   And gsy-e is resumed
   Then the gsye-e-sdk is connected to the gsy-e until finished
   And the gsye-e-sdk does not report errors
