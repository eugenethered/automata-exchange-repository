import logging
from typing import List, Optional

from core.exchange.ExchangeRate import ExchangeRate
from core.exchange.InstrumentExchange import InstrumentExchange
from core.options.exception.MissingOptionError import MissingOptionError
from exchange.rate.ExchangeRateHolder import ExchangeRateHolder
from exchange.rate.InstantRate import InstantRate
from timeseries.holder.InfluxDBHolder import InfluxDBHolder
from timeseries.provider.InfluxDBProvider import InfluxDBProvider

EXCHANGE_RATE_TIMESERIES_KEY = 'EXCHANGE_RATE_TIMESERIES_KEY'
EXCHANGE_RATE_TIMESERIES_RETENTION = 'EXCHANGE_RATE_TIMESERIES_RETENTION'


class ExchangeRateRepository:

    def __init__(self, options):
        self.log = logging.getLogger('ExchangeRateRepository')
        self.options = options
        self.__check_options()
        self.timeseries_key = options[EXCHANGE_RATE_TIMESERIES_KEY]
        self.timeseries_db: InfluxDBProvider = InfluxDBHolder()

    def __check_options(self):
        if self.options is None:
            self.log.warning(f'missing option please provide options [{EXCHANGE_RATE_TIMESERIES_KEY}]')
            raise MissingOptionError(f'missing option please provide options [{EXCHANGE_RATE_TIMESERIES_KEY}]')
        if EXCHANGE_RATE_TIMESERIES_KEY not in self.options:
            self.log.warning(f'missing option please provide option {EXCHANGE_RATE_TIMESERIES_KEY}')
            raise MissingOptionError(f'missing option please provide option {EXCHANGE_RATE_TIMESERIES_KEY}')

    @staticmethod
    def instrument_exchange_rate_key(instrument_exchange: InstrumentExchange):
        instruments_to_exchange = f'{instrument_exchange.instrument}/{instrument_exchange.to_instrument}'
        return instruments_to_exchange

    def store(self, exchange_rate: ExchangeRate, event_time):
        instrument_key = self.instrument_exchange_rate_key(exchange_rate)
        self.timeseries_db.add_to_timeseries(self.timeseries_key, instrument_key, exchange_rate.rate, event_time)

    def retrieve(self, instrument_exchange: InstrumentExchange, range_from='-6m', exchange_rate_holder: ExchangeRateHolder = ExchangeRateHolder()) -> ExchangeRateHolder:
        rate_timeseries_key = self.instrument_exchange_rate_key(instrument_exchange)
        timeseries_data = self.timeseries_db.get_timeseries_data(self.timeseries_key, rate_timeseries_key, range_from=range_from)
        for interval, rate in timeseries_data:
            exchange_rate = ExchangeRate(instrument_exchange.instrument, instrument_exchange.to_instrument, rate)
            exchange_rate_holder.add(exchange_rate, interval)
        return exchange_rate_holder

    def retrieve_multiple(self, instrument_exchanges: List[InstrumentExchange], range_from='-6m') -> ExchangeRateHolder:
        exchange_rate_holder = ExchangeRateHolder()
        for instrument_exchange in instrument_exchanges:
            self.retrieve(instrument_exchange, range_from, exchange_rate_holder)
        return exchange_rate_holder

    def retrieve_latest(self, instrument_exchange: InstrumentExchange, range_from='-6m') -> Optional[InstantRate]:
        rate_timeseries_key = self.instrument_exchange_rate_key(instrument_exchange)
        result = self.timeseries_db.get_latest_timeseries_data(self.timeseries_key, rate_timeseries_key, range_from=range_from)
        return None if result is None else InstantRate(result[0], result[1])

    def delete_all_exchange_rates(self):
        self.timeseries_db.delete_timeseries(self.timeseries_key)
