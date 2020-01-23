from abc import ABC, abstractmethod


class APIClientInterface(ABC):
    """
    Interface for D3A API clients, that support different communication protocols.
    This interface defines the common user functionality that these client should
    support.
    """

    @abstractmethod
    def __init__(self, market_id, client_id, *args, **kwargs):
        """
        On the constructor of the interface, it is obligatory for the user to provide
        the ID of the market that wants to access, and also to provide his client identifier
        so that D3A can authenticate the connected user.
        :param market_id: Identifier of the market to be connected to
        :param client_id: Identifier of the client
        :param kwargs:
        """
        pass

    @abstractmethod
    def register(self, is_blocking):
        """
        Registers the client to a D3A endpoint. According to the client might be part of
        the constructor, to allow the client to automatically register when creating
        the client object.
        :param is_blocking: Controls whether the client should wait for the registration process
        to finish or to not wait and poll manually or get notified by the event callback.
        :return: None
        """
        pass

    @abstractmethod
    def unregister(self, is_blocking):
        """
        Unregisters the connected client from the D3A endpoint.
        :param is_blocking: Controls whether the client should wait for the unregister process
        to finish or to not wait and poll manually or get notified by the event callback.
        :return: None
        """
        pass

    @abstractmethod
    def offer_energy(self, energy, price):
        """
        Places an energy offer to a registered market. Will fail if the client is not registered
        to the market. It is a blocking operation and will wait for the response of the market.
        :param energy: Energy of the offer, in kWh
        :param price: Price of the offer, in Euro cents
        :return: If the offer was created successfully, returns detailed information about the
        created offer. If the offer failed to be created successfully, returns a reason for
        the error.
        """
        pass

    @abstractmethod
    def bid_energy(self, energy, price):
        """
        Places an energy bid to a registered market. Will fail if the client is not registered
        to the market. It is a blocking operation and will wait for the response of the market.
        :param energy: Energy of the bid, in kWh
        :param price: Price of the bid, in Euro cents
        :return: If the bid was created successfully, returns detailed information about the
        created bid. If the bid failed to be created successfully, returns a reason for
        the error.
        """
        pass

    @abstractmethod
    def delete_offer(self, offer_id):
        """
        Deletes an offer that was posted on the market.
        :param offer_id: Id of the offer that was placed on the market. The id of the offer can
        be retrieved by the list_offers method or by the return value of the offer_energy method.
        :return: Status of the delete offer operation.
        """
        pass

    @abstractmethod
    def delete_bid(self, bid_id):
        """
        Deletes a bid that was posted on the market.
        :param bid_id: Id of the bid that was placed on the market. The id of the bid can
        be retrieved by the list_bids method or by the return value of the bid_energy method.
        :return: Status of the delete bid operation.
        """
        pass

    @abstractmethod
    def list_offers(self):
        """
        Lists all posted offers on the market from the client.
        :return: List of all the offers that this client has posted to the market.
        """
        pass

    @abstractmethod
    def list_bids(self):
        """
        Lists all posted bids on the market from the client.
        :return: List of all the bids that this client has posted to the market.
        """
        pass

    @abstractmethod
    def on_register(self, registration_info):
        """
        Method that is meant to be overridden, to allow custom user actions when registering the client.
        A user of the class should be able to override this method via subclassing. This method will
        be called in the background when the registration process has been completed.
        :param registration_info: Information about the markets and connection that get reported
        automatically when a client registers to the market.
        :return: None
        """
        pass

    def on_market_cycle(self, market_info):
        """
        Method that is meant to be overridden, to allow custom user actions when a new market is
        created. A user of the class should be able to override this method via subclassing.
        This method will be called in the background when a new market slot has been created.
        :param market_info: Information about the current market that gets reported automatically
        when a new market slot is created.
        :return: None
        """
        pass
