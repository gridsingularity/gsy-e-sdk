Feature: Device Tests

Scenario: API client can connect successfully to a load device and perform all operations
   Given redis container is started
   And d3a container is started using setup file strategy_tests.external_devices
   When the external client is started with test_load_connection
   Then the external client is connecting to the simulation until finished
   And the external client does not report errors
   And the energy bills of the load report the required energy was bought by the load

Scenario: API client can connect successfully to a PV device and perform all operations
   Given redis container is started
   And d3a container is started using setup file strategy_tests.external_devices
   When the external client is started with test_pv_connection
   Then the external client is connecting to the simulation until finished
   And the external client does not report errors
   And the on_event_or_response is called for different events

Scenario: External ESS agent not allowed to overcharge the Storage State
   Given redis container is started
   And d3a container is started using setup file strategy_tests.external_ess_bids
   When the external client is started with test_ess_bid_connection
   Then the external client is connecting to the simulation until finished
   And the external client does not report errors
   And the storage is not overcharged

Scenario: External ESS agent not allowed to sell below min_allowed_soc
   Given redis container is started
   And d3a container is started using setup file strategy_tests.external_ess_offers
   When the external client is started with test_ess_offer_connection
   Then the external client is connecting to the simulation until finished
   And the external client does not report errors
   And the storage state is limited to min_allowed_soc

Scenario: on_event_or_response is triggered by all messages
   Given redis container is started
   And d3a container is started using setup file strategy_tests.external_devices
   When the external client is started with test_pv_connection
   Then the external client is connecting to the simulation until finished
   And the external client does not report errors

Scenario: Batch Commands sent successfully
   Given redis container is started
   And d3a container is started using setup file strategy_tests.external_devices
   When the external client is started with test_aggregator_batch_commands
   Then the external client is connecting to the simulation until finished
   And the external client does not report errors