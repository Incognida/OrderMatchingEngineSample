from django.conf import settings
from rest_framework import serializers

from orders.models import Order
from posts.models import Post


class MarketsListSerializer(serializers.ListSerializer):
    child = serializers.JSONField(read_only=True)

class MarketDetailSerializer(serializers.ListSerializer):
    child = serializers.JSONField(read_only=True)

class CurrencyNameField(serializers.Field):
    def to_representation(self, obj):
        ret = {"BTC": "Bitcoin",
               "ETH": "Ethereum",
               "XRP": "Ripple",
               "EOS": "EOS",
               "NEO": "NEO"}
        return ret[obj["currency"]]

class PairInfoSerializer(serializers.Serializer):
    currency_name = CurrencyNameField(source="*")
    currency = serializers.CharField(read_only=True)
    volume = serializers.FloatField(read_only=True)
    change = serializers.FloatField(read_only=True)
    last_price = serializers.FloatField(read_only=True)
    day_high = serializers.FloatField(read_only=True)
    day_low = serializers.FloatField(read_only=True)
    spread = serializers.FloatField(read_only=True)

class ActiveUserOrdersSerializer(serializers.ModelSerializer):
    class Meta:
        model = Order
        fields = "__all__"

class AllUserOrdersSerializer(serializers.ModelSerializer):
    class Meta:
        model = Order
        fields = "__all__"

class SliderOrderSerializer(serializers.Serializer):
    currency_name = CurrencyNameField(source="*")
    currency = serializers.CharField(read_only=True)
    last_price = serializers.CharField(read_only=True)
    volume = serializers.CharField(read_only=True)

    class Meta:
        fields = ("currency", "last_price", "volume", "currency_name")

class SliderPostSerializer(serializers.ModelSerializer):
    class Meta:
        model = Post
        exclude = ("id", "visible")

class SliderSerializer(serializers.Serializer):
    order = SliderOrderSerializer(read_only=True)
    post = serializers.ListField(child=SliderPostSerializer())

    class Meta:
        fields = ("order", "post")

class GraphicOrderSerializer(serializers.Serializer):
    graphic = serializers.JSONField(read_only=True)

    class Meta:
        fields = ("graphic",)

class BooksOrderSerializer(serializers.Serializer):
    bids = serializers.JSONField(read_only=True)
    asks = serializers.JSONField(read_only=True)

    class Meta:
        fields = ("bids", "asks")
