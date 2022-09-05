from uberhf.datafeed.quote_info import QuoteInfo
from uberhf.orders.orders_common import OrderTimeInForce


class SmartOrderBase:

    def on_infrastructure_status(self):
        """
        Notification about ZMQ connection, Source status, OrderManager status, Order Gate Status, Trading session status
        """
        pass

    def on_initialize(self):
        """
        This method is called for initialization order healthy state, when the order was just created or loaded from the DB
        """
        pass

    def on_quote(self, q: QuoteInfo):
        """
        New quote update for one of the orders instruments just arrived
        """
        pass

    def on_instrument_info(self, iinfo: QuoteInfo):
        """
        Some instrument specification information has changed (i.e. margin requirements, price limits, tick value, theo price for options)
        """
        pass

    def on_order_status(self):
        """
        Reply for order status request
        """
        pass

    def on_execution_report(self):
        """
        Generic execution information as of FIX protocol
        """
        pass

    def on_cxlrep_reject(self):
        """
        Generic cancel/replace rejects as of FIX protocol
        """
        pass


if __name__ == '__main__':
    #OrderTime = OrderTimeInForce()
    print(OrderTime.FOK)
    print(OrderTime.DAY)
