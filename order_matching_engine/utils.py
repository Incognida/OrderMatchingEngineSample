import os
import django
from _decimal import Decimal

os.environ['DJANGO_SETTINGS_MODULE'] = 'cex_backend.settings'
django.setup()

from django_redis import get_redis_connection


r = get_redis_connection()


def get_order_from_redis(order_id):
    order_keys = r.hkeys(f"order_{order_id}")
    return_quote = {}
    for key in order_keys:
        key = key.decode()
        value = r.hget(f"order_{order_id}", key).decode()
        if key in ['quantity', 'price', 'initial_quantity']:
            value = Decimal(value)
        return_quote.update({key: value})
    return return_quote


def change_order(order_id, new_quantity):
    if Decimal(new_quantity) == 0:
        r.delete(f"order_{order_id}")
    else:
        r.hset(f"order_{order_id}", "quantity", new_quantity)


def get_currencies(quote):
    """
    :param quote: dict
    :return: main_currency, counter_currency: str
    """
    main_curr, second_curr = quote['pair'].split("_")
    if quote['side'] == 'bid':
        return main_curr, second_curr
    return second_curr, main_curr


def get_quantity(quote):
    """
    :param quote: dict
    :return: quantity: Decimal
    """
    if quote['side'] == 'bid':
        return quote['price'] * quote['quantity']
    return quote['quantity']


if __name__ == '__main__':
    pass
