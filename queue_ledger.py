class QueueLedger:
    def __init__(self, *a, **kw): pass
    def get_queue(self, ticker): return []
    def overwrite_queue(self, ticker, data): pass
    def add_lot(self, ticker, qty, price, desc=""): pass
    def pop_lots(self, ticker, qty, price=0.0): pass
    def clear_queue(self, ticker): pass
    def delete_lot(self, ticker, date): pass
    def edit_lot(self, ticker, date, qty, price): pass
    def sync_with_broker(self, ticker, qty, avg): pass
    def load_daily_snapshot(self, ticker): return None
