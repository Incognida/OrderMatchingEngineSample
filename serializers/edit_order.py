import socket
import json

from django.utils import timezone
from django.conf import settings
from rest_framework import serializers
from orders.order_matching_engine.utils import r, get_order_from_redis
from orders.models import Order
from .utils import positive_id, dec_to_str


class EditOrderSerializer(serializers.Serializer):
    order_id = serializers.IntegerField(required=True, validators=[positive_id])
    pair = serializers.ChoiceField(required=True, choices=settings.PAIRS)
    edited_quantity = serializers.DecimalField(
        required=False,
        decimal_places=10, max_digits=18
    )
    edited_price = serializers.DecimalField(
        required=False,
        decimal_places=10, max_digits=18
    )

    def edit_order(self):
        if self.is_valid():
            former_order_id = self.validated_data['order_id']
            pair = self.validated_data['pair']
            edited_quantity = self.validated_data.get('edited_quantity', 0)
            edited_price = self.validated_data.get('edited_price', 0)

            if not (edited_quantity or edited_price):
                return "You have to edit one of the fields"

            order = get_order_from_redis(former_order_id)
            if not order:
                return "Order was already completed/cancelled/edited"

            edited_quote = {
                "quantity": str(edited_quantity),
                "price": str(edited_price),
                "edited": True,
                "former_order_id": former_order_id,
                "timestamp": timezone.now().timestamp()
            }

            edited_quote = json.dumps(edited_quote)
            sock = socket.socket()
            sock.connect(('localhost', settings.SOCKET_PAIR_PORTS[pair]))
            sock.send(edited_quote.encode())
        else:
            return str(self.errors)
