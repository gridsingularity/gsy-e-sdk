test_simulation = {
    "username": "user@test.com",
    "name": "AggregatorTestSuite",
    "uuid": "some_random_uuid",
    "domain_name": "https://cool.webpage.com",
    "websockets_domain_name": "wss://cool.webpage.com/external-ws",
}

test_agg = {
    "name": "TestAggr",
    "uuid": "some_random_aggregator_uuid"
}

test_batch_command_dict = {"some_device_uuid": [{"command_dict0"},
                                                {"command_dict1"}],
                           "some_other_device_uuid": [{"command_dict2"},
                                                      {"command_dict3"}],
                           }

test_response = {"responses": {"asset_id_1": ["response1", "response2"],
                               "asset_id_2": ["response3", "response4"]}}

TEST_CONFIGURATION_PREFIX = "/configuration_prefix/"

TEST_AGGREGATOR_PREFIX = "/aggregator_prefix/"
