import os
import random
import sys
import time
import json
import socket
import django

from multiprocessing import Process, Queue as mpqueue
from threading import Thread, Event
from _decimal import Decimal
from collections import deque

from django.db import connections
from django.utils import timezone
from django.conf import settings
from djmoney.money import Money

from orders.order_matching_engine import OrderTree
from orders.order_matching_engine.money_manager import (change_assets,
                                                        MoneyManager,
                                                        can_handle)
from orders.order_matching_engine.utils import get_order_from_redis
from orders.models import Order
from userdata.models import CustomUser as User
from .db_writer import DBwriter
from .heapq_with_removal import HeapQueue
from .utils import change_order, r

os.environ['DJANGO_SETTINGS_MODULE'] = 'cex_backend.settings'
django.setup()


class SocketHandler(Thread):
    def __init__(self, pair, heap_queue):
        Thread.__init__(self)
        self._pair = pair
        self.heap_queue = heap_queue
        self._stopped = Event()

    def stop(self):
        self._stopped.set()

    def is_stopped(self):
        return self._stopped.is_set()

    def run(self):
        sock = socket.socket()
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('localhost', settings.SOCKET_PAIR_PORTS[self._pair]))
        sock.listen()
        while not self.is_stopped():
            conn, addr = sock.accept()
            message = conn.recv(256).decode()
            conn.close()
            if message == 'STOP':
                self.heap_queue.put(0, quote='STOP')
                self.stop()
            else:
                quote = json.loads(message)
                if quote.get("cancelled", False):
                    self.heap_queue.put(1, quote['timestamp'], quote)
                elif quote.get("edited", False):
                    self.heap_queue.put(2, quote['timestamp'], quote)
                else:
                    if r.hget("cancelled", f"{quote['order_id']}"):
                        # Сюда попадают ордера которые были отменены до их обработки
                        r.hdel("cancelled", f"{quote['order_id']}")
                        continue
                    order_type = quote['order_type']
                    if order_type == 'market':
                        self.heap_queue.put(3, quote['timestamp'], quote)
                    elif order_type == 'limit':
                        self.heap_queue.put(4, quote['timestamp'], quote)
        sock.close()


class OrderBook(Process):
    def __init__(self, pair):
        Process.__init__(self)
        self._pair = pair
        self.tape = deque(maxlen=None)  # Index[0] is most recent trade
        self.bids = OrderTree()
        self.asks = OrderTree()
        self.heap_queue = HeapQueue()
        self.total_time = 0

    def run_helper_processes(self):
        self.writer_mpqueue = mpqueue()
        self.writer = DBwriter(self.writer_mpqueue, self._pair)
        self.writer.start()

        self.socket_handler = SocketHandler(self._pair, self.heap_queue)
        self.socket_handler.start()

    def process_order(self, quote):
        if quote == 'STOP':
            self.socket_handler.join()
            self.writer_mpqueue.put(('stop', ''))
            self.writer.join()
            return False
        if quote.get('cancelled', False):
            self.cancel_order(quote['order_id'])
            return True
        elif quote.get("edited", False):
            self.edit_order(quote)
            return True

        quote['quantity'] = Decimal(quote['quantity'])
        quote['price'] = Decimal(quote['price'])
        quote['initial_quantity'] = Decimal(quote['initial_quantity'])
        # print(f"Incoming quote - {quote}")
        if quote['order_type'] == 'market':
            self.process_market_order(quote)
        else:
            self.process_limit_order(quote)
        # print(self)
        # print()
        return True

    def run(self):
        self.run_helper_processes()
        # fill the book with orders from RDB
        self.fill_book()
        r.set('{}_OrderBook_runs'.format(self._pair), True)

        while True:
            priority, timestamp, quote = self.heap_queue.get()
            ok = self.process_order(quote)
            if not ok or self.socket_is_stopped() or self.db_felt():
                break
        # TODO: Отменить ордера пользователей, чьи ордера находятся в очереди
        r.delete('{}_OrderBook_runs'.format(self._pair))
        self.log_book()
        sys.exit(0)

    def socket_is_stopped(self):
        if self.socket_handler.is_stopped():
            self.socket_handler.join()

            self.writer_mpqueue.put(('stop', ''))
            self.writer.join()
            return True
        return False

    def fill_book(self):
        orders = Order.objects.filter(
            pair=self._pair,
            quantity__gt=0,
            status='pending',
        )
        for order in orders:
            quote = {
                'user_id': order.user_id, 'pair': order.pair,
                'side': order.side, 'order_type': order.order_type,
                'initial_quantity': order.initial_quantity,
                'quantity': order.quantity, 'price': order.price,
                'timestamp': order.created_at.timestamp(),
                'order_id': order.pk,
            }
            r.hset(f"order_{quote['order_id']}", "at_book", True)
            self.bids.insert_order(quote) if quote['side'] == 'bid' \
                else self.asks.insert_order(quote)
        connections['default'].close()

    def process_order_list(self, side, order_list, quantity_still_to_trade,
                           quote):
        """
        Takes an OrderList (stack of orders at one price)
        and an incoming order and matches
        appropriate trades given the order's quantity.
        """
        trades = []
        quantity_to_trade = quantity_still_to_trade
        while order_list and quantity_to_trade > 0:
            head_order = order_list.get_head_order()
            head_order_id = head_order.order_id
            traded_price = head_order.price
            counter_party = head_order.user_id
            new_book_quantity = 0
            if quote['order_type'] == 'market' and quote['side'] == 'bid':
                ch_quant = head_order.quantity if \
                    quantity_to_trade <= head_order.quantity \
                    else quantity_to_trade
                to_be_checked = quote
                to_be_checked['price'] = head_order.price
                to_be_checked['quantity'] = ch_quant
                enough_assets = MoneyManager(to_be_checked).check_assets()
                if not enough_assets:
                    return quantity_to_trade, trades, False

            # print(f"Check for comparable:"
            #       f" type of q - {type(quantity_to_trade)}"
            #       f" type of hq - {type(head_order.quantity)}")

            if quantity_to_trade < head_order.quantity:
                traded_quantity = quantity_to_trade
                # Do the transaction
                new_book_quantity = head_order.quantity - quantity_to_trade
                head_order.update_quantity(new_book_quantity,
                                           head_order.timestamp)
                quantity_to_trade = 0
            elif quantity_to_trade == head_order.quantity:
                traded_quantity = quantity_to_trade
                if side == 'bid':
                    self.bids.remove_order_by_id(head_order.order_id)
                else:
                    self.asks.remove_order_by_id(head_order.order_id)
                quantity_to_trade = 0
            else:  # quantity to trade is larger than the head order
                traded_quantity = head_order.quantity
                if side == 'bid':
                    self.bids.remove_order_by_id(head_order.order_id)
                else:
                    self.asks.remove_order_by_id(head_order.order_id)
                quantity_to_trade -= traded_quantity

            # make a dict of head_order
            head_quote = head_order.__dict__
            head_quote['pair'] = self._pair
            head_quote['quantity'] = new_book_quantity

            # change quantities of orders at redis
            # current_hq = r.hget(f"order_{head_order_id}", "quantity")
            # current_oq = r.hget(f"order_{quote['order_id']}", "quantity")
            # print(f"Before change: head_q - {current_hq},"
            #       f"order_q - {current_oq}")
            change_order(head_order_id, str(new_book_quantity))
            change_order(quote['order_id'], str(quantity_to_trade))
            # changed_hq = r.hget(f"order_{head_order_id}", "quantity")
            # changed_oq = r.hget(f"order_{quote['order_id']}", "quantity")
            # print(f"After change: head_q - {changed_hq},"
            #       f"order_q - {changed_oq}")

            # change assets of users at redis and makes transactions to RDB
            order, head_order = change_assets(quote, head_quote, traded_quantity)
            self.writer_mpqueue.put(('match_transaction', [order, head_order]))

            # print(f"Head_quote quantity - {head_quote['quantity']}")
            # put changes of head_order to DBWriter's queue
            self.writer_mpqueue.put(('update', head_quote))

        return quantity_to_trade, trades, True

    def process_market_order(self, quote):
        trades = []
        quantity_to_trade = quote['quantity']
        side = quote['side']
        enough_assets = True

        if side == 'bid':
            while quantity_to_trade > 0 and self.asks and enough_assets:
                best_price_asks = self.asks.min_price_list()
                quantity_to_trade, new_trades, enough_assets = self.process_order_list(
                    'ask',
                    best_price_asks, quantity_to_trade, quote
                )
                trades += new_trades
        else:
            while quantity_to_trade > 0 and self.bids:
                best_price_bids = self.bids.max_price_list()
                quantity_to_trade, new_trades, _ = self.process_order_list(
                    'bid',
                    best_price_bids, quantity_to_trade, quote
                )
                trades += new_trades
        quote['quantity'] = quantity_to_trade

        # маркет бид не может удовлетворить требованиям ордеров в стакане
        if not enough_assets:
            # не размораживаем средства, так как нечего.
            r.delete(f"order_{quote['order_id']}")
            self.writer_mpqueue.put(('cancel', quote['order_id']))
            return trades

        if quote['quantity'] > 0 and quote['side'] == 'bid':
            # 1) Проверить есть ли цена в стакане
            # 2) Присвоить ордеру максимальную цену бида или дефолтную
            # 3) Проверить сможет ли пользователь потянуть ордер
            # 4) Если сможет заморозить средства,
            #    изменить цену ордера и добавить в дерево/ордерлист
            # 5) Не сможет - отдать оставшиеся средства,
            #    удалить ордер из редиса и отменить ордер в бд
            max_bid_price = self.bids.max_price()
            if max_bid_price:
                quote['price'] = Decimal(max_bid_price)
            else:
                quote['price'] = Decimal(
                    str(round(float(random.uniform(0.00000001, 10.0)), 9))
                )

            mm = MoneyManager(quote)
            if mm.check_assets():
                mm.freeze()
                r.hset(f"order_{quote['order_id']}", "price",
                       str(quote['price']))
                r.hset(f"order_{quote['order_id']}", "at_book", True)
                self.writer_mpqueue.put(('freeze', quote))
                self.bids.insert_order(quote)
            else:
                mm.refund()
                r.delete(f"order_{quote['order_id']}")
                self.writer_mpqueue.put(('cancel', quote['order_id']))
        elif quote['quantity'] > 0 and quote['side'] == 'ask':
            # 1) Присвоить ордеру минимальную цену аска или дефолтную
            # 2) Изменить цену ордера в редисе и добавить в дерево/ордерлист
            min_ask_price = self.asks.min_price()
            if min_ask_price:
                quote['price'] = Decimal(min_ask_price)
            else:
                quote['price'] = Decimal(
                    str(round(float(random.uniform(0.00000001, 10.0)), 9))
                )
            r.hset(f"order_{quote['order_id']}", "price", str(quote['price']))
            r.hset(f"order_{quote['order_id']}", "at_book", True)
            self.asks.insert_order(quote)
        self.writer_mpqueue.put(('update', quote))
        # print(trades)
        return trades

    def process_limit_order(self, quote):
        trades = []
        quantity_to_trade = quote['quantity']
        side = quote['side']
        price = quote['price']

        if side == 'bid':
            while self.asks and price >= self.asks.min_price() and quantity_to_trade > 0:
                best_price_asks = self.asks.min_price_list()
                # print(f"Best_price_asks\n{best_price_asks}")
                quantity_to_trade, new_trades, _ = self.process_order_list(
                    'ask',
                    best_price_asks, quantity_to_trade, quote
                )
                trades += new_trades
            # If volume remains, need to update the book with new quantity
            if quantity_to_trade > 0:
                quote['quantity'] = quantity_to_trade
                r.hset(f"order_{quote['order_id']}", "at_book", True)
                self.bids.insert_order(quote)
        else:
            while self.bids and price <= self.bids.max_price() and quantity_to_trade > 0:
                best_price_bids = self.bids.max_price_list()
                # print(f"Best_price_asks\n{best_price_bids}")
                quantity_to_trade, new_trades, _ = self.process_order_list(
                    'bid',
                    best_price_bids, quantity_to_trade, quote
                )
                trades += new_trades
            # If volume remains, need to update the book with new quantity
            if quantity_to_trade > 0:
                quote['quantity'] = quantity_to_trade
                r.hset(f"order_{quote['order_id']}", "at_book", True)
                self.asks.insert_order(quote)

        # Pass the order to write into DB
        # if it wasn't changed, so don't rewrite
        quote['quantity'] = quantity_to_trade
        initial_quantity = quote['initial_quantity']
        if quantity_to_trade != initial_quantity:
            self.writer_mpqueue.put(('update', quote))
        # print(f"Trades done - {trades}")
        return trades

    def cancel_order_at_book_db(self, order_id, edited=False):
        exists = self.bids.order_exists(order_id)
        if exists:
            self.bids.remove_order_by_id(order_id)
        else:
            exists = self.asks.order_exists(order_id)
            if exists:
                self.asks.remove_order_by_id(order_id)
        if exists and edited:
            self.writer_mpqueue.put(('edit', order_id))
        elif exists:
            self.writer_mpqueue.put(('cancel', order_id))
        else:
            return False
        return True

    def modify_order(self, order_id, order_update):
        side = order_update['side']
        order_update['order_id'] = order_id
        order_update['timestamp'] = timezone.now().timestamp()
        if side == 'bid':
            if self.bids.order_exists(order_update['order_id']):
                self.bids.update_order(order_update)
        else:
            if self.asks.order_exists(order_update['order_id']):
                self.asks.update_order(order_update)

    def get_volume_at_price(self, side, price):
        price = Decimal(price)
        if side == 'bid':
            volume = 0
            if self.bids.price_exists(price):
                volume = self.bids.get_price_list(price).volume
            return volume
        else:
            volume = 0
            if self.asks.price_exists(price):
                volume = self.asks.get_price_list(price).volume
            return volume

    def get_best_bid(self):
        return self.bids.max_price()

    def get_worst_bid(self):
        return self.bids.min_price()

    def get_best_ask(self):
        return self.asks.min_price()

    def get_worst_ask(self):
        return self.asks.max_price()

    def tape_dump(self, filename, filemode, tapemode):
        dumpfile = open(filename, filemode)
        for tapeitem in self.tape:
            to_dump = f"""Time: {tapeitem['time']}, 
                          Price: {tapeitem['price']}, 
                          Quantity: {tapeitem['quantity']}"""
            dumpfile.write(to_dump)
        dumpfile.close()
        if tapemode == 'wipe':
            self.tape = []

    def __str__(self):
        return_value = "*ORDERBOOK*\n"
        return_value += "Bids left\n"
        if self.bids and len(self.bids) > 0:
            return_value += "q\tp\t\n"
            for key, value in self.bids.price_tree.items(reverse=True):
                return_value += '%s' % value
        return_value += "Asks left\n"
        if self.asks is not None and len(self.asks) > 0:
            return_value += "q\tp\t\n"
            for key, value in list(self.asks.price_tree.items()):
                return_value += '%s' % value
        return_value += "*ORDERBOOK*"
        return return_value

    def log_book(self):
        pf = os.path.dirname(os.path.abspath(__file__))
        with open(f"{pf}/order_book_logs/{self._pair}_OrderBook_trades.txt",
                  'w') as f:
            f.write("***Bids left***\n")
            if self.bids is not None and len(self.bids) > 0:
                f.write("quantity@price\t user_id\t - timestamp\n")
                for key, value in self.bids.price_tree.items(reverse=True):
                    f.write('%s' % value)
            f.write("\n***Asks left***\n")
            if self.asks is not None and len(self.asks) > 0:
                f.write("quantity@price\t user_id\t - timestamp\n")
                for key, value in list(self.asks.price_tree.items()):
                    f.write('%s' % value)
            f.write("\n***Trades done***\n")
            if self.tape is not None and len(self.tape) > 0:
                f.write("quantity @ price\t time\t party1/party2\n")
                for entry in self.tape:
                    content = f"{entry['quantity']} @ {entry['price']}\t" \
                              f" ({entry['time']})\t {entry['party1'][0]} / " \
                              f"{entry['party2'][0]}\n"
                    f.write(content)
            f.write("\n")

    def db_felt(self):
        if r.get("db_stopped"):
            # Остановить DBWriter
            self.writer_mpqueue.put(('stop', ''))
            self.writer.join()

            # Остановить SocketHandler
            sock = socket.socket()
            try:
                sock.connect(
                    ('localhost', settings.SOCKET_PAIR_PORTS[self._pair])
                )
                sock.send(b'STOP')
                sock.close()
            except ConnectionRefusedError:
                pass
            self.socket_handler.join()
            return True
        return False

    def cancel_order(self, order_id, edited=False):
        """
        1) удалить ордер в стакане/очереди
        2) отдать на запись в БД
        3) сделать рефанд в редисе
           если это маркет бид - пропустить этот шаг
        4) удалить ордер в редисе
        """
        quote = get_order_from_redis(order_id)
        # В стакане
        if quote and quote.get("at_book", False):
            self.cancel_order_at_book_db(order_id, edited)
            mm = MoneyManager(quote, cancelled=True)
            self.writer_mpqueue.put(('cancel_transaction', quote))
            mm.refund()
        # В очереди
        elif quote:
            # Когда тред вытащит ордер - он его пропустит
            r.hset("cancelled", f"{order_id}", order_id)
            if edited:
                self.writer_mpqueue.put(('edit', order_id))
            else:
                self.writer_mpqueue.put(('cancel', order_id))
            if not (quote['order_type'] == 'market' and quote['side'] == 'bid'):
                mm = MoneyManager(quote, cancelled=True)
                self.writer_mpqueue.put(('cancel_transaction', quote))
                mm.refund()
            # иначе не делать рефанд, т.к. маркет бид еще не попал в стакан
        r.delete(f"order_{order_id}")

    def edit_order(self, edited_quote):
        """
        1) проверить сможет ли пользователь выдержать ордер
        2) если не сможет - пропустить ордер, иначе - дальше3) сделать отмену прошлого ордера4) захостить новый ордер"""
        from orders.serializers.create_order import CreateOrderSerializer
        current_order_id = edited_quote['former_order_id']
        current_quote = get_order_from_redis(current_order_id)
        order_type = current_quote['order_type']

        edited_quantity = Decimal(edited_quote['quantity'])
        edited_price = Decimal(edited_quote['price'])
        edited_quote['quantity'] = edited_quantity
        edited_quote['price'] = edited_price

        result = can_handle(edited_quote, current_quote)
        if not result:
            return None
        self.cancel_order(current_order_id, edited=True)
        if edited_price:
            order_type = 'limit'
        else:
            edited_price = current_quote['price']
        if not edited_quantity:
            edited_quantity = current_quote['quantity']

        new_quote = {
            'user_id': int(current_quote['user_id']),
            'pair': current_quote['pair'],
            'side': current_quote['side'],
            'order_type': order_type,
            'quantity': edited_quantity,
            'price': edited_price
        }
        errors = CreateOrderSerializer(data=new_quote).host_order()
        if errors:
            print(f"Errors occurred - {errors}")


if __name__ == '__main__':
    print("This file should not be called")
