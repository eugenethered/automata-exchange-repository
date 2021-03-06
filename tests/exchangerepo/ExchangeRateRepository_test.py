import unittest

from cache.holder.RedisCacheHolder import RedisCacheHolder
from cache.provider.RedisCacheProviderWithTimeSeries import RedisCacheProviderWithTimeSeries
from core.exchange.ExchangeRate import ExchangeRate
from core.exchange.InstrumentExchange import InstrumentExchange
from core.number.BigFloat import BigFloat
from exchange.rate.InstantRate import InstantRate

from exchangerepo.repository.ExchangeRateRepository import ExchangeRateRepository


class ExchangeRateRepositoryTestCase(unittest.TestCase):

    def setUp(self) -> None:
        options = {
            'REDIS_SERVER_ADDRESS': '10.104.71.60',
            'REDIS_SERVER_PORT': 6379,
            'EXCHANGE_RATE_TIMESERIES_KEY': 'test:ts:exchange-rate:{}',
            'EXCHANGE_RATE_TIMESERIES_RETENTION': 3600000
        }
        self.cache = RedisCacheHolder(options, held_type=RedisCacheProviderWithTimeSeries)
        self.repository = ExchangeRateRepository(options)

    def tearDown(self):
        self.cache.delete_timeseries('test:ts:exchange-rate:BTC/USDT', double_precision=True)
        self.cache.delete_timeseries('test:ts:exchange-rate:ETH/USDT', double_precision=True)

    def test_should_store_exchange_rates_via_repo(self):
        self.repository.store(ExchangeRate('BTC', 'USDT', BigFloat('38835.34')), 1)
        self.repository.store(ExchangeRate('BTC', 'USDT', BigFloat('38719.72')), 2)
        exchange_rates = self.repository.retrieve(InstrumentExchange('BTC', 'USDT'), 0, 3)
        rates = exchange_rates.get_rates('BTC', 'USDT')
        self.assertEqual(len(rates), 2)
        self.assertEqual(rates[0], InstantRate(2, BigFloat('38719.72')))
        self.assertEqual(rates[1], InstantRate(1, BigFloat('38835.34')))

    def test_should_obtain_exchange_rates_using_latest_available_range_end_interval(self):
        self.repository.store(ExchangeRate('BTC', 'USDT', BigFloat('38835.34')), 1)
        self.repository.store(ExchangeRate('BTC', 'USDT', BigFloat('38719.72')), 2)
        exchange_rates = self.repository.retrieve(InstrumentExchange('BTC', 'USDT'), 0)
        rates = exchange_rates.get_rates('BTC', 'USDT')
        self.assertEqual(len(rates), 2)
        self.assertEqual(rates[0], InstantRate(2, BigFloat('38719.72')))
        self.assertEqual(rates[1], InstantRate(1, BigFloat('38835.34')))

    def test_should_obtain_latest_exchange_rate(self):
        self.repository.store(ExchangeRate('BTC', 'USDT', BigFloat('38835.34')), 1)
        self.repository.store(ExchangeRate('BTC', 'USDT', BigFloat('38719.72')), 2)
        latest_exchange_rate = self.repository.retrieve_latest(InstrumentExchange('BTC', 'USDT'))
        self.assertEqual(latest_exchange_rate, InstantRate(2, BigFloat('38719.72')))
        self.assertEqual(latest_exchange_rate.rate.invert(), BigFloat('0.000025826633043833'))

    def test_should_not_obtain_latest_exchange_rate_when_there_are_none(self):
        latest_exchange_rate = self.repository.retrieve_latest(InstrumentExchange('BTC', 'USDT'))
        self.assertEqual(latest_exchange_rate, None)

    def test_should_have_empty_exchange_rates_from_repo_when_instruments_not_available(self):
        exchange_rates = self.repository.retrieve(InstrumentExchange('BTC', 'USDT'), 0, 3)
        rates = exchange_rates.get()
        self.assertEqual(rates, {})

    def test_should_obtain_multiple_exchange_rates_via_repo(self):
        self.repository.store(ExchangeRate('BTC', 'USDT', BigFloat('38835.34')), 1)
        self.repository.store(ExchangeRate('BTC', 'USDT', BigFloat('38719.72')), 2)
        self.repository.store(ExchangeRate('ETH', 'USDT', BigFloat('2861.62')), 1)
        self.repository.store(ExchangeRate('ETH', 'USDT', BigFloat('2870.19')), 2)
        instrument_exchanges = [
            InstrumentExchange('BTC', 'USDT'),
            InstrumentExchange('ETH', 'USDT')
        ]
        exchange_rates = self.repository.retrieve_multiple(instrument_exchanges, 0, 3)
        rates_1 = exchange_rates.get_rates('BTC', 'USDT')
        self.assertEqual(len(rates_1), 2)
        self.assertEqual(rates_1[0], InstantRate(2, BigFloat('38719.72')))
        self.assertEqual(rates_1[1], InstantRate(1, BigFloat('38835.34')))
        rates_2 = exchange_rates.get_rates('ETH', 'USDT')
        self.assertEqual(len(rates_2), 2)
        self.assertEqual(rates_2[0], InstantRate(2, BigFloat('2870.19')))
        self.assertEqual(rates_2[1], InstantRate(1, BigFloat('2861.62')))

    def test_should_obtain_multiple_exchange_rates_using_latest_available_range_end_interval(self):
        self.repository.store(ExchangeRate('BTC', 'USDT', BigFloat('38835.34')), 1)
        self.repository.store(ExchangeRate('BTC', 'USDT', BigFloat('38719.72')), 2)
        self.repository.store(ExchangeRate('ETH', 'USDT', BigFloat('2861.62')), 1)
        self.repository.store(ExchangeRate('ETH', 'USDT', BigFloat('2870.19')), 2)
        instrument_exchanges = [
            InstrumentExchange('BTC', 'USDT'),
            InstrumentExchange('ETH', 'USDT')
        ]
        exchange_rates = self.repository.retrieve_multiple(instrument_exchanges, 0)
        rates_1 = exchange_rates.get_rates('BTC', 'USDT')
        self.assertEqual(len(rates_1), 2)
        self.assertEqual(rates_1[0], InstantRate(2, BigFloat('38719.72')))
        self.assertEqual(rates_1[1], InstantRate(1, BigFloat('38835.34')))
        rates_2 = exchange_rates.get_rates('ETH', 'USDT')
        self.assertEqual(len(rates_2), 2)
        self.assertEqual(rates_2[0], InstantRate(2, BigFloat('2870.19')))
        self.assertEqual(rates_2[1], InstantRate(1, BigFloat('2861.62')))


if __name__ == '__main__':
    unittest.main()
