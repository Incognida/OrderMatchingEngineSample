import json
import os

from multiprocessing import Process
from _decimal import Decimal
from django.utils import timezone
from django.conf import settings
from django.db import transaction
from djmoney.money import Money
from orders.order_matching_engine.utils import r, get_quantity, get_currencies
from orders.order_matching_engine.money_manager import MoneyManager
from orders.serializers.utils import dec_to_str
from orders.models import Order


class DBwriter(Process):
    def __init__(self, queue, pair):
        Process.__init__(self)
        self._queue = queue
        self._pair = pair

    def run(self):
        while True:
            # Если какой-либо другой DBWriter упал
            if r.get("db_stopped"):
                self.dump_queue()
                break

            command, quote = self._queue.get()
            try:
                if command == 'update':
                    self._update_order(quote)
                elif command == 'cancel':
                    order_id = quote
                    self._cancel_order(order_id)
                elif command == 'edit':
                    order_id = quote
                    self._cancel_order(order_id, edited=True)
                elif command == 'freeze':
                    self.freeze(quote)
                elif command == 'match_transaction':
                    incoming_quote = quote[0]
                    head_quote = quote[1]
                    matcher = Matcher(incoming_quote, head_quote)
                    matcher.change_assets()
                elif command == 'cancel_transaction':
                    self._cancel_transaction(quote)
                elif command == 'stop':
                    break
            except Exception as e:
                r.set("db_stopped", True)
                pf = os.path.join(os.path.dirname(__file__), 'db_errors.txt')
                with open(pf, 'a') as f:
                    f.write(f"time - {timezone.now().timestamp()}, error - {e}, command - {command}, quote - {quote}\n\n")
                # Дамп выпавшего ордера
                if isinstance(quote, int):
                    quote = {
                        "order_id": quote, "cancelled": True,
                        "command": command
                    }
                elif isinstance(quote, list):
                    quote = {
                        "command": "match",
                        "quote1": dec_to_str(quote[0]),
                        "quote2": dec_to_str(quote[1])
                     }
                else:
                    quote.update({"command": command})
                    quote = dec_to_str(quote)
                quote.update({"fallen": True})

                # Дамп очереди
                self.dump_queue(fallen_order=quote)
                break

    def dump_queue(self, fallen_order=None):
        """
        Закинуть все ордера в очереди в файл.

        Прим. первым ордером в файле - будет выпавший ордер (если у него
        есть ключ "fallen"),
        или не будет - если этот процесс нормально работал
        до остановки базы с другого процесса
        """
        pt_data = []
        if fallen_order:
            pt_data.append(fallen_order)
        while True:
            command, quote = self._queue.get()
            if command == 'stop':
                break
            if isinstance(quote, int):
                quote = {
                    "order_id": quote, "cancelled": True,
                    "command": command
                }
            elif isinstance(quote, list):
                quote = {
                    "command": "match",
                    "quote1": dec_to_str(quote[0]),
                    "quote2": dec_to_str(quote[1])
                }
            else:
                quote.update({"command": command})
                quote = dec_to_str(quote)
            pt_data.append(quote)

        if pt_data:
            dumped_data = json.dumps(pt_data)
            pf = os.path.join(
                os.path.dirname(__file__),
                f"{self._pair}_dmp_q.json"
            )
            with open(pf, 'w') as f:
                f.write(dumped_data)

    @staticmethod
    def _update_order(quote):
        order_id = quote['order_id']
        quantity = Decimal(quote['quantity'])
        order_qs = Order.objects.filter(pk=order_id)
        if quantity == 0:
            if quote['order_type'] == 'market' and quote['price'] != 0:
                order_qs.update(
                    status='completed',
                    price=quote['price'],
                    quantity=0, closed_at=timezone.now()
                )
            else:
                order_qs.update(
                    status='completed', quantity=0, closed_at=timezone.now()
                )
        else:
            if quote['order_type'] == 'market' and quote['price'] != 0:
                order_qs.update(
                    price=quote['price'], quantity=quantity
                )
            else:
                order_qs.update(quantity=quantity)
        order_qs.first().save()
        return None

    @staticmethod
    def _cancel_order(order_id, edited=False):
        order_qs = Order.objects.filter(pk=order_id)
        order = order_qs.first()
        if edited:
            order_qs.update(
                status='edited', closed_at=timezone.now()
            )
            order.save()
        else:
            order_qs.update(
                status='cancelled', closed_at=timezone.now()
            )
            order.save()

    @staticmethod
    def freeze(quote):
        from transactions.models import (InternalTransactionBTC,
                                         InternalTransactionETH,
                                         InternalTransactionXRP,
                                         InternalTransactionEOS)
        from cryptocurrency.models import WalletBTC, WalletETH, WalletXRP
        main_curr, fil_curr = get_currencies(quote)
        amount = Money(get_quantity(quote), main_curr)
        comm_amount = Decimal(settings.DEFAULT_COMMISSION) * amount.amount
        comm_amount = Money(comm_amount, main_curr)

        query_str = f"""
        InternalTransaction{main_curr}.objects.create(
            user_id=quote['user_id'], order_id=quote['order_id'],
            category='freeze', amount=amount,
            commission_amount=comm_amount,
            wallet=Wallet{main_curr}.objects.get(user_id=quote['user_id'])
        )
        """.strip()
        eval(query_str)

    @staticmethod
    def _cancel_transaction(quote):
        from transactions.models import (InternalTransactionBTC,
                                         InternalTransactionETH,
                                         InternalTransactionXRP,
                                         InternalTransactionEOS)
        from cryptocurrency.models import WalletBTC, WalletETH, WalletXRP
        main_curr, fil_curr = get_currencies(quote)
        amount = Money(get_quantity(quote), main_curr)

        tx = eval(f"InternalTransaction{main_curr}")
        wallet = eval(f"Wallet{main_curr}")

        comm_amount = Decimal(settings.DEFAULT_COMMISSION) * amount.amount
        comm_amount = Money(comm_amount, main_curr)

        tx.objects.create(
            user_id=quote['user_id'], order_id=quote['order_id'],
            category='cancel_bet', amount=amount,
            commission_amount=
            comm_amount if 0 < quote['quantity'] < quote['initial_quantity']
            else Money(Decimal(0), main_curr),
            tx_type='incoming',
            wallet=wallet.objects.get(user_id=quote['user_id'])
        )


class Matcher:
    def __init__(self, incoming_quote, head_quote):
        self.o = MoneyManager(incoming_quote)
        self.h = MoneyManager(head_quote)
        self.o_id = incoming_quote['order_id']
        self.h_id = head_quote['order_id']

    class FuckingORM:
        def __init__(self, **kwargs):
            self.curr = kwargs['curr']
            self.user_id = kwargs['user_id']
            self.order_id = kwargs['order_id']
            self.quant = kwargs['quant']
            self.comm = kwargs.get('comm', Decimal(0))
            self.tx_type = kwargs['tx_type']

        def create_tx(self):
            from transactions.models import (InternalTransactionBTC,
                                             InternalTransactionETH,
                                             InternalTransactionXRP,
                                             InternalTransactionEOS)
            from cryptocurrency.models import WalletBTC, WalletETH, WalletXRP
            query = f"""
            InternalTransaction{self.curr}.objects.create(
                user_id=self.user_id, order_id=self.order_id,
                category='match', 
                amount=Money(self.quant, self.curr), 
                commission_amount=Money(self.comm, self.curr), 
                wallet=Wallet{self.curr}.objects.get(user_id=self.user_id),
                tx_type=self.tx_type
            )
            """.strip()
            eval(query)

    @transaction.atomic
    def change_assets(self):
        # Example: USD_BTC
        # q     p            side               q       p       side
        # 5     undefined    BID                3       6500    ASK
        if self.o.side == 'bid':
            # curr     amount
            # USD      - 3*6500  (minus commission if it's market bid)
            commission = Decimal(0)
            if self.o.side == 'bid' and self.o.order_type == 'market':
                commission = Decimal(settings.DEFAULT_COMMISSION) * self.o.total_quantity
            query = {
                'curr': self.o.curr,
                'user_id': self.o.user_id, 'order_id': self.o_id,
                'quant': self.o.total_quantity, 'comm': commission,
                'tx_type': 'reduction'
            }
            self.FuckingORM(**query).create_tx()

            # curr     amount
            # BTC      + 3
            query['curr'] = self.o.counter_curr
            query['quant'] = self.o.traded_quantity
            query['comm'] = Decimal(0)
            query['tx_type'] = 'incoming'
            self.FuckingORM(**query).create_tx()

            # curr      amount
            # BTC       - 3
            query = {
                'curr': self.h.curr,
                'user_id': self.h.user_id, 'order_id': self.h_id,
                'quant': self.h.traded_quantity,
                'tx_type': 'reduction'
            }
            self.FuckingORM(**query).create_tx()

            # curr      amount
            # USD       + 3*6500
            query['curr'] = self.h.counter_curr
            query['quant'] = self.h.total_quantity
            query['tx_type'] = 'incoming'
            self.FuckingORM(**query).create_tx()
        elif self.o.side == 'ask':
            # curr      amount
            # BTC       - 3
            query = {
                'curr': self.o.curr,
                'user_id': self.o.user_id, 'order_id': self.o_id,
                'quant': self.o.traded_quantity,
                'tx_type': 'reduction'
            }
            self.FuckingORM(**query).create_tx()

            # curr      amount
            # USD       + 3*6500
            query['curr'] = self.o.counter_curr
            query['quant'] = self.o.total_quantity
            query['tx_type'] = 'incoming'
            self.FuckingORM(**query).create_tx()

            # curr     amount
            # USD      - 3*6500
            query = {
                'curr': self.h.curr,
                'user_id': self.h.user_id, 'order_id': self.h_id,
                'quant': self.h.total_quantity,
                'tx_type': 'reduction'
            }
            self.FuckingORM(**query).create_tx()

            # curr     amount
            # BTC      + 3
            query['curr'] = self.h.counter_curr
            query['quant'] = self.h.traded_quantity
            query['tx_type'] = 'incoming'
            self.FuckingORM(**query).create_tx()


if __name__ == '__main__':
    print('This file should not be called explicitly')
