import socket
import json
from django.utils import timezone
from django.conf import settings
from rest_framework import serializers
from orders.order_matching_engine.utils import r
from .utils import positive_id


class CancelOrderSerializer(serializers.Serializer):
    order_id = serializers.IntegerField(required=True, validators=[positive_id])
    pair = serializers.ChoiceField(choices=settings.PAIRS, required=True)

    def cancel_order(self):
        if self.is_valid():
            order_id = self.validated_data['order_id']
            if not r.hget(f"order_{order_id}", "order_id"):
                return "Order was already completed/cancelled/edited"

            pair = self.validated_data['pair']
            quote = json.dumps({
                "order_id": order_id,
                "timestamp": timezone.now().timestamp(),
                "cancelled": True
            })

            sock = socket.socket()
            sock.connect(('localhost', settings.SOCKET_PAIR_PORTS[pair]))
            sock.send(quote.encode())
        else:
            return str(self.errors)
