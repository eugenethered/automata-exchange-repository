import unittest

from cache.holder.RedisCacheHolder import RedisCacheHolder
from core.exchange.ExchangeRate import ExchangeRate
from core.exchange.InstrumentExchange import InstrumentExchange
from core.number.BigFloat import BigFloat
from exchange.rate.InstantRate import InstantRate

from exchangeraterepo.repository.ExchangeRateRepository import ExchangeRateRepository


class ExchangeRateRepositoryTestCase(unittest.TestCase):

    def setUp(self) -> None:
        options = {
            'REDIS_SERVER_ADDRESS': '192.168.1.90',
            'REDIS_SERVER_PORT': 6379,
            'TIMESERIES_KEY': 'test-time-series:exchange-rate:{}'
        }
        self.cache = RedisCacheHolder(options)
        self.repository = ExchangeRateRepository(options)

    def tearDown(self):
        self.cache.delete_timeseries('test-time-series:exchange-rate:BTC/USDT', double_precision=True)

    def test_should_store_exchange_rates_via_repo(self):
        self.repository.store(ExchangeRate('BTC', 'USDT', BigFloat('38835.34')), 1)
        self.repository.store(ExchangeRate('BTC', 'USDT', BigFloat('38719.72')), 2)
        exchange_rates = self.repository.retrieve(InstrumentExchange('BTC', 'USDT'), 0, 3)
        rates = exchange_rates.get_rates('BTC', 'USDT')
        self.assertEqual(len(rates), 2)
        self.assertEqual(rates[0], InstantRate(1, BigFloat('38835.34')))
        self.assertEqual(rates[1], InstantRate(2, BigFloat('38719.72')))

    def test_should_have_empty_exchange_rates_from_repo_when_instruments_not_available(self):
        exchange_rates = self.repository.retrieve(InstrumentExchange('BTC', 'USDT'), 0, 3)
        rates = exchange_rates.get()
        self.assertEqual(rates, {})


if __name__ == '__main__':
    unittest.main()
