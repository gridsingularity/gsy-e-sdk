Feature: Device Tests

Scenario: API client can connect successfully to a load device and perform all operations
   Given redis container is started
   And gsy-e is started using setup strategy_tests.external_devices (-t 60s -s 60m -d 4h --slot-length-realtime 2s)
   When the external client is started with test_aggregator_load
   Then the external client is connecting to the simulation until finished
   And the external client does not report errors

Scenario: API client can connect successfully to a PV device and perform all operations
   Given redis container is started
   And gsy-e is started using setup strategy_tests.external_devices (-t 60s -s 60m -d 9h --slot-length-realtime 2s)
   When the external client is started with test_aggregator_pv
   Then the external client is connecting to the simulation until finished
   And the external client does not report errors

Scenario: Aggregator can connect to ESS and place bids and offers
   Given redis container is started
   And gsy-e is started using setup strategy_tests.external_ess_offers (-t 60s -s 60m -d 4h --slot-length-realtime 2s)
   When the external client is started with test_aggregator_ess
   Then the external client is connecting to the simulation until finished
   And the external client does not report errors
