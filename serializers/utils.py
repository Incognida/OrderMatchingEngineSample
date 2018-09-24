from _decimal import Decimal

from rest_framework import serializers


def positive_id(value):
    if value > 0:
        return True
    else:
        raise serializers.ValidationError('order_id interval (0, +INF)')


def dec_to_str(quote):
    another_quote = {}
    another_quote.update(quote)
    for key, value in another_quote.items():
        if isinstance(value, Decimal):
            another_quote[key] = str(value)
    return another_quote


def str_to_dec(quote):
    another_quote = {}
    another_quote.update(quote)
    another_quote['quantity'] = str(another_quote['quantity'])
    another_quote['initial_quantity'] = str(another_quote['initial_quantity'])
    another_quote['price'] = str(another_quote['price'])
    return another_quote


if __name__ == '__main__':
    pass
