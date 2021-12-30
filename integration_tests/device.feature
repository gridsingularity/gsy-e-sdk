Feature: Device Tests

Scenario: API client can connect successfully to a load device and perform all operations
   Given redis container is started
   And gsy-e is started in paused mode using setup strategy_tests.external_devices (-t 60s -s 60m -d 4h --slot-length-realtime 2s)
   When the gsy-e-sdk is connecting to gsy-e with test_aggregator_load
   And gsy-e is resumed
   And the gsy-e-sdk is connected to the gsy-e until finished
   And the gsy-e-sdk does not report errors

Scenario: API client can connect successfully to a PV device and perform all operations
   Given redis container is started
   And gsy-e is started in paused mode using setup strategy_tests.external_devices (-t 60s -s 60m -d 9h --slot-length-realtime 2s)
   When the gsy-e-sdk is connecting to gsy-e with test_aggregator_pv
   And gsy-e is resumed
   And the gsy-e-sdk is connected to the gsy-e until finished
   And the gsy-e-sdk does not report errors

Scenario: Aggregator can connect to ESS and place bids and offers
   Given redis container is started
   And gsy-e is started in paused mode using setup strategy_tests.external_ess_offers (-t 60s -s 60m -d 4h --slot-length-realtime 2s)
   When the gsy-e-sdk is connecting to gsy-e with test_aggregator_ess
   And gsy-e is resumed
   And the gsy-e-sdk is connected to the gsy-e until finished
   And the gsy-e-sdk does not report errors
