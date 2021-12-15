Feature: Market Tests

Scenario: API client can connect successfully to a market and perform all operations
  Given redis container is started
  And gsy-e is started in paused mode using setup strategy_tests.external_market_stats (-t 60s -s 60m -d 4h --slot-length-realtime 2s)
  When the gsy-e-sdk is connected house-2 market on gsy-e
  And gsy-e is resumed
  And the gsye-e-sdk is connected to the gsy-e until finished
  Then the gsye-e-sdk does not report errors
