Feature: Aggregator Tests

Scenario: Batch Commands sent successfully
   Given redis container is started
   And d3a container is started using setup file strategy_tests.external_devices
   When the external client is started with test_aggregator_batch_commands
   Then the external client is connecting to the simulation until finished
   And the external client does not report errors

Scenario: Grid fees are correctly calculated
   Given redis container is started
   And d3a container started strategy_tests.external_devices_grid_fees with duration set to 3 hours
   When the external client is started with test_aggregator_batch_commands
   Then the external client is connecting to the simulation until finished
   And the grid fees are correctly calculated