Feature: Settlement Market Tests

Scenario: Assets can post bids and offers
  Given redis container is started
  And gsy-e is started in paused mode using setup strategy_tests.external_devices_settlement_market (-t 60s -s 60m -d 4h --slot-length-realtime 2s)
  When the gsy-e-sdk is connecting to gsy-e with test_aggregator_settlement
  And gsy-e is resumed
  And the gsy-e-sdk is connected to the gsy-e until finished
  And the gsy-e-sdk does not report errors
