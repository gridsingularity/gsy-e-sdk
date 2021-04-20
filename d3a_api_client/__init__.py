from abc import ABC, abstractmethod


class APIClientInterface(ABC):
    """
    Interface for D3A API clients, that support different communication protocols.
    This interface defines the common user functionality that these clients should
    support.
    """

    @abstractmethod
    def __init__(self, area_id, *args, **kwargs):
        """
        On the constructor of the interface, it is obligatory for the user to provide
        the ID of the market that wants to access, and also to provide his client identifier
        so that D3A can authenticate the connected user.
        :param area_id: Identifier of the market to be connected to
        :param kwargs:
        """
        pass

    @abstractmethod
    def register(self):
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

    def on_event_or_response(self, message):
        """
       Method that is meant to be overridden, to allow custom user actions when any event or response is
        received by the client.
       A user of the class should be able to override this method via subclassing. This method will
       be called in the background when an event or response is received.
       :param message: Information about the event or response that was reported
       :return: None
        """
        pass
