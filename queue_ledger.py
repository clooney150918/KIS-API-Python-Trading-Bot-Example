# stub: queue_ledger.py - removed per cleanup
class QueueLedger:
    def __init__(self, *args, **kwargs):
        pass
    def get_queue(self, ticker, default=None):
        return default if default is not None else []
    def clear_queue(self, ticker):
        pass
    def sync_with_broker(self, ticker, *args, **kwargs):
        pass
    def overwrite_queue(self, ticker, data):
        pass
    def apply_stock_split(self, ticker, ratio, *args, **kwargs):
        pass
