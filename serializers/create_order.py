from _decimal import Decimal

import json
import socket

from djmoney.money import Money
from rest_framework import serializers
from django.utils import timezone
from django.conf import settings
from orders.order_matching_engine.money_manager import MoneyManager
from orders.order_matching_engine.utils import r, get_currencies, get_quantity
from orders.models import Order
from transactions.models import (InternalTransactionBTC, InternalTransactionEOS,
                                 InternalTransactionERC20Token, InternalTransactionETH,
                                 InternalTransactionXRP)
from cryptocurrency.models.wallets import WalletBTC, WalletETH, WalletXRP
from .utils import dec_to_str


def create_at_redis(quote):
    order_id = quote['order_id']
    pipe = r.pipeline()
    pipe.hset(f"order_{order_id}", "order_id", order_id)
    pipe.hset(f"order_{order_id}", "user_id", quote['user_id'])
    pipe.hset(f"order_{order_id}", "pair", quote['pair'])
    pipe.hset(f"order_{order_id}", "side", quote['side'])
    pipe.hset(f"order_{order_id}", "order_type", quote['order_type'])
    pipe.hset(f"order_{order_id}", "quantity", str(quote['quantity']))
    pipe.hset(f"order_{order_id}", "initial_quantity", str(quote['quantity']))
    pipe.hset(f"order_{order_id}", "price", str(quote['price']))
    pipe.hset(f"order_{order_id}", "timestamp", quote['timestamp'])
    pipe.execute()


def create_order(quote):
    market_bid = False
    quantity = quote['quantity']
    user_id = int(quote['user_id'])

    if quote['order_type'] == 'market' and quote['side'] == 'bid':
        market_bid = True
    else:
        mm = MoneyManager(quote)
        enough_assets = mm.check_assets()
        if not enough_assets:
            return "not enough assets"
        mm.freeze()

    quote.update({'initial_quantity': quantity})
    try:
        order = Order.objects.create(**quote)
        if not market_bid:
            main_curr, fil_cur = get_currencies(order.__dict__)
            amount = Money(get_quantity(order.__dict__), main_curr)
            comm_amount = Decimal(settings.DEFAULT_COMMISSION) * amount.amount
            comm_amount = Money(comm_amount, main_curr)
            print(f"{main_curr} {amount.amount} {Decimal(settings.DEFAULT_COMMISSION) * amount.amount} {comm_amount}")
            query_str = f"""
                    InternalTransaction{main_curr}.objects.create(
                        user_id=user_id, order_id=order.pk,
                        category='freeze', amount=amount,
                        commission_amount=comm_amount,
                        wallet=Wallet{main_curr}.objects.get(user_id=user_id)
                    )
                    """.strip()
            eval(query_str)
    # if db falls
    except Exception as e:
        if not market_bid:
            mm.refund()
        with open(f"{quote['pair']}_creation_error.json", 'a') as f:
            quote['timestamp'] = timezone.now().timestamp()
            quote = dec_to_str(quote)
            f.write(json.dumps(quote) + "\n")
            f.write(f"Error occurred - {e}")
        return None

    few_items = {
        'order_id': order.pk,
        'timestamp': order.created_at.timestamp()
    }
    quote.update(few_items)

    create_at_redis(quote)

    pair = quote['pair']
    quote = dec_to_str(quote)
    quote = json.dumps(quote)

    sock = socket.socket()
    sock.connect(('localhost', settings.SOCKET_PAIR_PORTS[pair]))
    sock.send(quote.encode())
    return None


class CreateOrderSerializer(serializers.Serializer):
    user_id = serializers.IntegerField(required=True)
    pair = serializers.ChoiceField(choices=settings.PAIRS, required=True)
    side = serializers.ChoiceField(choices=settings.SIDES, required=True)
    order_type = serializers.ChoiceField(
        choices=settings.ORDER_TYPES, required=True
    )
    quantity = serializers.DecimalField(
        max_digits=18, decimal_places=10, required=True
    )
    price = serializers.DecimalField(
        max_digits=18, decimal_places=10, required=False
    )

    def host_order(self):
        """
        1. Check requested data
        2. Check assets of user
        3. Create record in RDB
        4. Update quote with a (record's id, timestamp)
        5. Write to redis
        5. Send quote to daemon via socket

        Sent quote sample:
        {
            "order_id": 1,
            "user_id": 1,
            "pair": "BTC_ETH",
            "side": "bid",
            "order_type": "limit",
            "price": "0.125"
            "quantity": "4",
            "initial_quantity": "4",
            "timestamp": 1532590590.3393712
        }

        :return: "not enough assets"
                 "invalid data"
                 None - OK
        """
        if self.is_valid():
            if r.get("db_stopped"):
                return "Sorry, we cannot host orders atm"
            if self.validated_data['order_type'] == 'market':
                self.validated_data['price'] = Decimal(0)
            return create_order(quote=self.validated_data)
            # try:
            #     if r.get("db_stopped"):
            #         return "Sorry, we cannot host orders atm"
            #     if self.validated_data['order_type'] == 'market':
            #         self.validated_data['price'] = Decimal(0)
            #     return create_order(quote=self.validated_data)
            # except Exception as e:
            #     with open('serializer_errors.txt', 'a') as f:
            #         f.write(str(e) + "\n")
        else:
            return self.errors
