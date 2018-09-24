import datetime
import random
import dateutils
import logging
from django.conf import settings
from django.db.models import Sum, Max, Min, Count
from django.shortcuts import get_object_or_404
from django.utils import timezone
from rest_framework.generics import (CreateAPIView, RetrieveAPIView,
                                     ListAPIView)
from rest_framework.exceptions import APIException
from rest_framework.response import Response
from rest_framework.permissions import AllowAny
from .serializers import (CreateOrderSerializer, CancelOrderSerializer,
                          MarketsListSerializer, MarketDetailSerializer,
                          EditOrderSerializer, ActiveUserOrdersSerializer,
                          AllUserOrdersSerializer, SliderSerializer)

from .serializers.pairs_info import (GraphicOrderSerializer, PairInfoSerializer,
                                     BooksOrderSerializer)


from .models import Order
from posts.models import Post
from .order_matching_engine.utils import r


PAIRS = getattr(settings, "PAIRS", ('BTC_ETH', 'BTC_XRP', 'BTC_EOS', 'BTC_NEO',
                                    'ETH_XRP', 'ETH_EOS', 'ETH_NEO', 'XRP_EOS',
                                    'XRP_NEO', 'EOS_NEO'))

logging.basicConfig(filename="cex.log", level=logging.INFO, format = u'%(asctime)s - %(levelname): %(message)s')
_logger = logging.getLogger(__file__)


class CreateOrderView(CreateAPIView):
    """
    Создание ордера:
    ---
        {
            "user_id": 1,
            "pair": "BTC_ETH",
            "side": "bid",
            "order_type": "limit",
            "quantity": "16",
            "price": "0.125"
        },
        {
            "user_id": 2,
            "pair": "BTC_ETH",
            "side": "ask",
            "order_type": "market",
            "quantity": "8",
            "price": "0"
        }
    ---
    """
    permission_classes = (AllowAny,)
    serializer_class = CreateOrderSerializer

    def get_serializer_context(self):
        return {'request': self.request}

    def create(self, request, *args, **kwargs):
        serializer = self.serializer_class(
            data=request.data,
            context=self.get_serializer_context()
        )
        error = serializer.host_order()
        if error:
            return Response({"error": error}, status=400)
        return Response(status=200)


class CancelOrderView(CreateAPIView):
    """
    Отмена ордера:
    ---
        {
            "order_id": 1,
        }
    ---
    """
    permission_classes = (AllowAny,)
    serializer_class = CancelOrderSerializer

    def create(self, request, *args, **kwargs):
        serializer = self.serializer_class(data=request.data)
        error = serializer.cancel_order()
        if error:
            return Response({"error": error}, status=400)
        return Response(status=200)


class EditOrderView(CreateAPIView):
    """
    Изменение ордера:
    ---
        {
            "order_id": 1,
            "edited_quantity": "8",
            "edited_price": "0.14"
        }
    ---
    """
    permission_classes = (AllowAny,)
    serializer_class = EditOrderSerializer

    def create(self, request, *args, **kwargs):
        serializer = self.serializer_class(data=request.data)
        error = serializer.edit_order()
        if error:
            return Response({"error": error})
        return Response(status=200)

class MarketsView(ListAPIView):
    serializer_class = MarketsListSerializer
    permission_classes = (AllowAny,)

    def get_queryset(self):
        ret = {"BTC": "Bitcoin",
               "ETH": "Ethereum",
               "XRP": "Ripple"}
        return [{"currency": currency, "currency_name": ret[currency]} for currency in settings.CURRENCIES]

    def list(self, request, *args, **kwargs):
        queryset = self.filter_queryset(self.get_queryset())
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=False)
            return self.get_paginated_response(serializer.data)
        serializer = self.get_serializer(queryset, many=False)
        return Response(serializer.data)

class MarketDetailView(ListAPIView):
    serializer_class = MarketDetailSerializer
    permission_classes = (AllowAny,)

    def get_queryset(self):
        if self.market_name in settings.CURRENCIES:
            return [pair for pair in settings.PAIRS if self.market_name in pair.split("_")]
        else:
            raise APIException("Unknown currency")

    def list(self, request, *args, **kwargs):
        self.market_name = kwargs["market_name"]
        queryset = self.filter_queryset(self.get_queryset())
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=False)
            return self.get_paginated_response(serializer.data)
        serializer = self.get_serializer(queryset, many=False)
        return Response(serializer.data)

class ActiveUserOrdersView(ListAPIView):
    queryset = Order.objects.exclude(status__in=('completed', 'cancelled'))
    serializer_class = ActiveUserOrdersSerializer

    def filter_queryset(self, queryset):
        return queryset.filter(user=self.request.user)

class ActiveUserOrdersByPairView(ListAPIView):
    queryset = Order.objects.exclude(status__in=('completed', 'cancelled'))
    serializer_class = ActiveUserOrdersSerializer

    def filter_queryset(self, queryset):
        return queryset.filter(user=self.request.user)

    def list(self, request, *args, **kwargs):
        pair = kwargs.get("pair", None)
        if pair is not None:
            self.queryset = self.queryset.filter(pair=pair)
        return super().list(request, *args, **kwargs)

class AllUserOrdersView(ListAPIView):
    queryset = Order.objects.all()
    serializer_class = AllUserOrdersSerializer

    def filter_queryset(self, queryset):
        return queryset.filter(user=self.request.user)

class SliderOrdersView(RetrieveAPIView):
    queryset = Order.objects.all()
    serializer_class = SliderSerializer
    permission_classes = (AllowAny,)

    def get_object(self):
        queryset = self.get_queryset()
        random_currency = random.choice(settings.CURRENCIES)
        last_price = 0.0
        total_quantity = 0.0
        pairs = list(filter(lambda x: random_currency in x.split("_"), PAIRS))
        queryset = queryset.filter(pair__in=pairs, created_at__gte=datetime.datetime.now() + dateutils.relativedelta(hours=-24))
        if queryset.exists():
            total_quantity = queryset.aggregate(Sum("quantity"))["quantity__sum"]
            last_price = queryset.reverse()[0].price
        return {"order": {"currency": random_currency, "last_price": last_price, "volume": total_quantity}, "post": Post.objects.filter(language=self.lang, visible=True)}

    def retrieve(self, request, *args, **kwargs):
        self.lang = kwargs.pop("lang", "ru")
        _logger.info("META - %s" % (request.META))
        instance = self.get_object()
        try:
            serializer = self.get_serializer(instance)
            return Response(serializer.data)
        except Exception as err:
            raise APIException(err)

class PairInfoView(ListAPIView):
    queryset = Order.objects.all()
    serializer_class = PairInfoSerializer
    permission_classes = (AllowAny,)

    def get_queryset(self):
        if self.pair in settings.PAIRS:
            percent_change = 0.0
            last_price = 0.0
            percent_spread = 0.0
            orders = self.queryset
            pair = self.pair
            previous_orders = orders.filter(pair=pair, created_at__gte=datetime.datetime.now() + dateutils.relativedelta(hours=-48), created_at__lt=datetime.datetime.now() + dateutils.relativedelta(hours=-24)).exclude(status__in=('completed', 'cancelled')).order_by("-created_at")
            orders = orders.filter(pair=pair, created_at__gte=datetime.datetime.now() + dateutils.relativedelta(hours=-24)).exclude(status__in=('completed', 'cancelled')).order_by("-created_at")
            bids_order = orders.filter(side="bid")
            asks_order = orders.filter(side="ask")
            volume = bids_order.aggregate(Sum("quantity"))["quantity__sum"] or 0
            max_price = bids_order.aggregate(Max("price"))["price__max"] or 0
            min_price = bids_order.aggregate(Min("price"))["price__min"] or 0
            best_bid_price = max_price or 0
            best_ask_price = asks_order.aggregate(Min("price"))["price__min"] or 0
            spread = best_ask_price - best_bid_price
            if bids_order.count():
                last_order = bids_order[0]
                last_price = last_order.price or 0
                if previous_orders.count():
                    last_prev_price = previous_orders.filter(side="bid")[0].price
                    if last_prev_price:
                        percent_change = (last_price-last_prev_price) * 100/last_prev_price
                    previous_best_bid_price = previous_orders.filter(side="bid").aggregate(Max("price"))["price__max"] or 0
                    previous_best_ask_price = previous_orders.filter(side="ask").aggregate(Min("price"))["price__min"] or 0
                    previous_spread = previous_best_ask_price - previous_best_bid_price
                    if previous_spread:
                        percent_spread = (spread - previous_spread) * 100/previous_spread
            return {"currency": pair.split("_")[-1], "volume": float(volume), "change": percent_change, "last_price": float(last_price), "day_high": float(max_price), "day_low": float(min_price), "spread": percent_spread}
        else:
            raise APIException("Unknown pair")

    def list(self, request, *args, **kwargs):
        self.pair = kwargs["pair"]
        queryset = self.filter_queryset(self.get_queryset())
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=False)
            return self.get_paginated_response(serializer.data)
        serializer = self.get_serializer(queryset, many=False)
        return Response(serializer.data)

class GraphicOrderView(RetrieveAPIView):
    queryset = Order.objects.filter(side="bid").exclude(status__in=('completed', 'cancelled')).order_by("created_at")
    serializer_class = GraphicOrderSerializer
    permission_classes = (AllowAny,)

    def __init__(self, *args, **kwargs):
        self._periods = {
            "1_min": {"minutes": 1},
            "5_min": {"minutes": 5},
            "15_min": {"minutes": 15},
            "30_min": {"minutes": 30},
            "1_hour": {"hours": 1},
            "4_hour": {"hours": 4},
            "1_day": {"days": 1},
            "3_day": {"days": 3},
            "1_week": {"weeks": 1},
        }
        self.period = "1_min"
        super().__init__(*args, **kwargs)

    def get_info(self, *args, **kwargs):
        queryset = Order.objects.filter(pair=self.pair, created_at__gte=timezone.now() - datetime.timedelta(hours=24), created_at__lte=timezone.now()).exclude(status__in=('completed', 'cancelled')).order_by("-created_at")
        bid_orders = queryset.filter(side="bid")
        ask_orders = queryset.filter(side="ask")
        total_quantity_bid = bid_orders.aggregate(Sum("quantity"))["quantity__sum"] or 0
        last_bid_price = bid_orders.first().price if bid_orders.exists() else 0
        last_ask_price = ask_orders.first().price if ask_orders.exists() else 0
        max_bid_price = bid_orders.aggregate(Max("price"))["price__max"] or 0
        min_bid_price = bid_orders.aggregate(Min("price"))["price__min"] or 0
        last_price = Order.objects.filter(status="completed").order_by("-created_at")[0].price
        return [self.pair in self.request.user.pairs_list, last_price, last_bid_price, last_ask_price, total_quantity_bid, max_bid_price, min_bid_price]

    def filter_queryset(self, queryset):
        return queryset.filter(pair=self.pair)

    def get_object(self):
        queryset = self.get_queryset()
        queryset = self.filter_queryset(queryset)
        graphic_list = []
        current_datetime = timezone.now()
        _periods = self._periods
        if queryset.exists():
            first_order_date = queryset.first().created_at
            while current_datetime>=first_order_date:
                qqq = queryset.filter(created_at__lte=current_datetime, created_at__gte=current_datetime-datetime.timedelta(**_periods[self.period]))
                if qqq.exists():
                    open_price = qqq[0].price or 0
                    last_price = qqq.reverse()[0].price or 0
                    max_price = qqq.aggregate(Max("price"))["price__max"] or 0
                    min_price = qqq.aggregate(Min("price"))["price__min"] or 0
                    # graphic_list.append([open_price, max_price, min_price, last_price])
                    graphic_list.append({"open_price": open_price,
                                         "max_price": max_price,
                                         "min_price": min_price,
                                         "last_price": last_price,
                                         "date": current_datetime})
                else:
                    # graphic_list.append([0, 0, 0, 0])
                    graphic_list.append({"open_price": 0,
                                         "max_price": 0,
                                         "min_price": 0,
                                         "last_price": 0,
                                         "date": current_datetime})
                current_datetime -= datetime.timedelta(**_periods[self.period])
        return {"graphic": graphic_list}

    def retrieve(self, request, *args, **kwargs):
        self.period = request.GET.get("period")
        self.pair = request.GET.get("pair")
        return super().retrieve(request, *args, **kwargs)

class BooksOrderView(RetrieveAPIView):
    serializer_class = BooksOrderSerializer

    def get_object(self):
        pipe = r.pipeline()
        orders = r.keys("order_*")
        bid_orders = []
        ask_orders = []
        for order in orders:
            data_order = r.hgetall(order)
            data = {}
            for key, value in data_order.items():
                data[str(key)] = value
            if r.hget(order, "side") == b"bid":
                bid_orders.append(data)
            elif r.hget(order, "side") == b"ask":
                ask_orders.append(data)
        return {"bids": bid_orders, "asks": ask_orders}
