Feature: Batch Commands Tests

Scenario: Batch Commands sent successfully
   Given redis container is started
   And gsy-e is started using setup strategy_tests.external_devices (-t 60s -s 60m -d 12h --slot-length-realtime 2s)
   When the external client is started with test_aggregator_batch_commands
   Then the external client is connecting to the simulation until finished
   And the external client does not report errors
