import logging
import os
import django
from _decimal import Decimal

from django.conf import settings

from orders.order_matching_engine.utils import r

os.environ['DJANGO_SETTINGS_MODULE'] = 'cex_backend.settings'
django.setup()


logging.basicConfig(filename='money_manager.log', filemode='a',
                    level=logging.INFO)
log = logging.getLogger('ex')


class MoneyManager:
    def __init__(self, data, cancelled=False):
        """
        Redis money manager.
        Manages money of user for each order that was modified/created
        :param data - order dict with changed/current price and/or quantity
        """
        try:
            self.user_id = data['user_id']
            self.side = data['side']
            self.order_type = data['order_type']
            self.traded_price = Decimal(data['price'])
            self.traded_quantity = Decimal(data['quantity'])
            self.total_quantity = self.traded_price * self.traded_quantity
            main_curr, second_curr = data['pair'].split('_')
            self.curr = main_curr if self.side == 'bid' else second_curr
            self.counter_curr = second_curr if self.side == 'bid' else main_curr
            self.bid_commission = self.total_quantity * Decimal(
                settings.DEFAULT_COMMISSION
            )
            self.ask_commission = self.traded_quantity * Decimal(
                settings.DEFAULT_COMMISSION
            )

            # Если ордер отменяется,
            # то traded_quantity - это и есть нынешний объем ордера
            if cancelled and self.traded_quantity != Decimal(data['initial_quantity']):
                self.quantity_triggered = True
        except Exception as e:
            log.exception(f"Error occurred - {e}")

    def check_assets(self):
        """
        Compare user's active assets and the desired order
        :return: True - enough assets
                 False - not enough
        """
        try:
            active_assets = r.get(f"active_{self.curr}_{self.user_id}")
            if not active_assets:
                r.set(f"active_{self.curr}_{self.user_id}", 0)
                return False
            active_assets = Decimal(active_assets.decode())

            if self.side == 'bid' and active_assets >= self.total_quantity + self.bid_commission:
                return True
            if self.side == 'ask' and active_assets >= self.traded_quantity + self.ask_commission:
                return True
            return False
        except Exception as e:
            log.exception(f"Error_occurred - {e}")

    def freeze(self):
        """
        Market bid
        pass

        Limit bid
        q   p     side      frozen += q*p
        5   6500  bid       active -= q*p

        Market/Limit ask
        q   p     side      frozen += q
        5   6500  ask       active -= q
        """
        # TODO: at view don't check market bid
        try:
            if self.side == 'bid':
                r.incrbyfloat(
                    f"frozen_{self.curr}_{self.user_id}",
                    self.total_quantity + self.bid_commission
                )
                r.incrbyfloat(
                    f"active_{self.curr}_{self.user_id}",
                    f"-{self.total_quantity + self.bid_commission}"
                )

            if self.side == 'ask':
                r.incrbyfloat(
                    f"frozen_{self.curr}_{self.user_id}",
                    self.traded_quantity + self.ask_commission
                )
                r.incrbyfloat(
                    f"active_{self.curr}_{self.user_id}",
                    f"-{self.traded_quantity + self.ask_commission}"
                )
        except Exception as e:
            log.exception(f"Error occurred - {e}")

    def refund(self):
        try:
            if not hasattr(self, 'quantity_triggered'):
                # Если объем менялся - не возвращаем коммиссию
                self.bid_commission = Decimal(0)
                self.ask_commission = Decimal(0)
            if self.side == 'bid':
                r.incrbyfloat(
                    f"frozen_{self.curr}_{self.user_id}",
                    f"-{self.total_quantity + self.bid_commission}"
                )
                r.incrbyfloat(
                    f"active_{self.curr}_{self.user_id}",
                    self.total_quantity + self.bid_commission
                )
            if self.side == 'ask':
                r.incrbyfloat(
                    f"frozen_{self.curr}_{self.user_id}",
                    f"-{self.traded_quantity + self.ask_commission}"
                )
                r.incrbyfloat(
                    f"active_{self.curr}_{self.user_id}",
                    self.traded_quantity + self.ask_commission
                )
        except Exception as e:
            log.exception(f"Error occurred - {e}")

    def __str__(self):
        return_value = "*MoneyManager*\n"
        returned_dict = {}
        for key, value in self.__dict__.items():
            returned_dict[key] = (value, type(value))
        return_value += str(returned_dict) + str("\n")
        return_value += "*MoneyManager*\n"
        return return_value


def cancel_order(order_id):
    pipe = r.pipeline()
    pipe.hset(f"cancelled", f"{order_id}", order_id)
    pipe.delete(f"order_{order_id}")


def change_assets(order, head_order, traded_quantity):
    try:
        another_order = {}
        another_head_order = {}
        another_order.update(order)
        another_head_order.update(head_order)
        another_order['price'] = another_head_order['price']
        another_order['quantity'] = traded_quantity
        another_head_order['quantity'] = traded_quantity
        # log.info(msg=f"another_head_order q"
        #              f" - {another_head_order['quantity']}, "
        #              f"head_order q - {head_order['quantity']}")
        o = MoneyManager(another_order)
        # log.info(msg=f"order info:\n{o}")
        h = MoneyManager(another_head_order)
        # log.info(msg=f"order info:\n{h}")

        if o.order_type == 'market' and o.side == 'bid':
            pipe = r.pipeline()
            pipe.incrbyfloat(
                f"active_{o.curr}_{o.user_id}",
                f"-{o.total_quantity + o.bid_commission}"
            )
            pipe.incrbyfloat(
                f"active_{o.counter_curr}_{o.user_id}",
                o.traded_quantity
            )

            pipe.incrbyfloat(
                f"frozen_{h.curr}_{h.user_id}",
                f"-{h.traded_quantity}"
            )
            pipe.incrbyfloat(
                f"active_{h.counter_curr}_{h.user_id}",
                h.total_quantity
            )
            pipe.execute()
            return another_order, another_head_order

        if o.side == 'bid':
            pipe = r.pipeline()
            pipe.incrbyfloat(
                f"frozen_{o.curr}_{o.user_id}",
                f"-{o.total_quantity}"
            )
            pipe.incrbyfloat(
                f"active_{o.counter_curr}_{o.user_id}",
                o.traded_quantity
            )

            pipe.incrbyfloat(
                f"frozen_{h.curr}_{h.user_id}",
                f"-{h.traded_quantity}"
            )
            pipe.incrbyfloat(
                f"active_{h.counter_curr}_{h.user_id}",
                h.total_quantity
            )
            pipe.execute()
        elif o.side == 'ask':
            pipe = r.pipeline()
            pipe.incrbyfloat(
                f"frozen_{o.curr}_{o.user_id}",
                f"-{o.traded_quantity}"
            )
            pipe.incrbyfloat(
                f"active_{o.counter_curr}_{o.user_id}",
                o.total_quantity
            )

            pipe.incrbyfloat(
                f"frozen_{h.curr}_{h.user_id}",
                f"-{h.total_quantity}"
            )
            pipe.incrbyfloat(
                f"active_{h.counter_curr}_{h.user_id}",
                h.traded_quantity
            )
            pipe.execute()
        return another_order, another_head_order
    except Exception as e:
        # with open('money_manager.txt', 'a') as f:
        #     f.write(str(e) + "\n")
        log.exception(f"Error occurred - {e}")


def can_handle(new_quote, quote):
    edited_price = None
    edited_quantity = None

    user_id = quote['user_id']
    side = quote['side']
    order_type = quote['order_type']
    price = quote['price']
    quantity = quote['quantity']
    main_curr, second_curr = quote['pair'].split('_')
    curr = main_curr if side == 'bid' else second_curr
    current_assets = Decimal(r.get(f"active_{curr}_{user_id}").decode())

    if new_quote['price'] > 0:
        edited_price = new_quote['price']
    if new_quote['quantity'] > 0:
        edited_quantity = new_quote['quantity']

    if not edited_price:
        edited_price = price
    if not edited_quantity:
        edited_quantity = quantity

    status = True
    if order_type == 'market':
        if side == 'bid' and edited_price != 0:
            if edited_price * edited_quantity > current_assets + price * quantity:
                status = False
        elif side == 'ask':
            if edited_quantity > current_assets + quantity:
                status = False
    elif order_type == 'limit':
        if side == 'bid':
            if edited_price * edited_quantity > current_assets + price * quantity:
                status = False
        elif side == 'ask':
            if edited_quantity > current_assets + quantity:
                status = False
    return status
