Feature: Aggregator Tests

Scenario: Batch Commands sent successfully
   Given redis container is started
   And d3a is started using setup strategy_tests.external_devices (-t 1s -s 60m)
   When the external client is started with test_aggregator_batch_commands
   Then the external client is connecting to the simulation until finished
   And the external client does not report errors

Scenario: Grid fees are correctly calculated
   Given redis container is started
   And d3a is started using setup strategy_tests.external_devices_grid_fees (-t 1s -s 60m -d 4h)
   When the external client is started with test_aggregator_batch_commands
   Then the external client is connecting to the simulation until finished
   And the grid fees are correctly calculated