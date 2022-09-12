import unittest

from core.exchange.ExchangeRate import ExchangeRate
from core.exchange.InstrumentExchange import InstrumentExchange
from core.number.BigFloat import BigFloat
from coreutility.date.NanoTimestamp import NanoTimestamp
from exchange.rate.InstantRate import InstantRate
from timeseries.holder.InfluxDBHolder import InfluxDBHolder

from exchangerepo.repository.ExchangeRateRepository import ExchangeRateRepository


class ExchangeRateRepositoryTestCase(unittest.TestCase):

    def setUp(self) -> None:
        options = {
            'INFLUXDB_SERVER_ADDRESS': '127.0.0.1',
            'INFLUXDB_SERVER_PORT': 8086,
            'INFLUXDB_AUTH_TOKEN': 'q3cfJCCyfo4RNJuyg72U-3uEhrv3qkKQcDOesoyeIDg2BCUpmn-mjReqaGwO7GOebhd58wYVkopi5tcgCj8t5w==',
            'INFLUXDB_AUTH_ORG': 'persuader-technology',
            'INFLUXDB_BUCKET': 'automata',
            'EXCHANGE_RATE_TIMESERIES_KEY': 'exchange-rate'
        }
        self.timeseries_db = InfluxDBHolder(options)
        self.repository = ExchangeRateRepository(options)

    def tearDown(self):
        self.repository.delete_all_exchange_rates()

    def test_should_store_exchange_rates_via_repo(self):
        self.repository.store(ExchangeRate('BTC', 'USDT', BigFloat('38835.34')), 1662842954870516619)
        self.repository.store(ExchangeRate('BTC', 'USDT', BigFloat('38719.72')), 1662842975769185669)
        exchange_rates = self.repository.retrieve(InstrumentExchange('BTC', 'USDT'), range_from='-365d')
        rates = exchange_rates.get_rates('BTC', 'USDT')
        self.assertEqual(len(rates), 2)
        self.assertEqual(rates[0], InstantRate(NanoTimestamp.as_shorted_nanoseconds(1662842975769185669), BigFloat('38719.72')))
        self.assertEqual(rates[1], InstantRate(NanoTimestamp.as_shorted_nanoseconds(1662842954870516619), BigFloat('38835.34')))

    def test_should_obtain_latest_exchange_rate(self):
        self.repository.store(ExchangeRate('BTC', 'USDT', BigFloat('38835.34')), 1662842954870516619)
        self.repository.store(ExchangeRate('BTC', 'USDT', BigFloat('38719.72')), 1662842975769185669)
        latest_exchange_rate = self.repository.retrieve_latest(InstrumentExchange('BTC', 'USDT'), range_from='-365d')
        self.assertEqual(latest_exchange_rate, InstantRate(1662842975769185000, BigFloat('38719.72')))
        self.assertEqual(latest_exchange_rate.rate.invert(), BigFloat('0.000025826633043833'))

    def test_should_not_obtain_latest_exchange_rate_when_there_are_none(self):
        latest_exchange_rate = self.repository.retrieve_latest(InstrumentExchange('BTC', 'USDT'))
        self.assertEqual(latest_exchange_rate, None)

    def test_should_have_empty_exchange_rates_from_repo_when_instruments_not_available(self):
        exchange_rates = self.repository.retrieve(InstrumentExchange('BTC', 'USDT'))
        rates = exchange_rates.get()
        self.assertEqual(rates, {})

    def test_should_obtain_multiple_exchange_rates_via_repo(self):
        self.repository.store(ExchangeRate('BTC', 'USDT', BigFloat('38835.34')), 1662842954870516619)
        self.repository.store(ExchangeRate('BTC', 'USDT', BigFloat('38719.72')), 1662842975769185669)
        self.repository.store(ExchangeRate('ETH', 'USDT', BigFloat('2861.62')), 1662842954870516619)
        self.repository.store(ExchangeRate('ETH', 'USDT', BigFloat('2870.19')), 1662842975769185669)
        instrument_exchanges = [
            InstrumentExchange('BTC', 'USDT'),
            InstrumentExchange('ETH', 'USDT')
        ]
        exchange_rates = self.repository.retrieve_multiple(instrument_exchanges, range_from='-365d')
        rates_1 = exchange_rates.get_rates('BTC', 'USDT')
        self.assertEqual(len(rates_1), 2)
        self.assertEqual(rates_1[0], InstantRate(1662842975769185000, BigFloat('38719.72')))
        self.assertEqual(rates_1[1], InstantRate(1662842954870516000, BigFloat('38835.34')))
        rates_2 = exchange_rates.get_rates('ETH', 'USDT')
        self.assertEqual(len(rates_2), 2)
        self.assertEqual(rates_2[0], InstantRate(1662842975769185000, BigFloat('2870.19')))
        self.assertEqual(rates_2[1], InstantRate(1662842954870516000, BigFloat('2861.62')))

    def test_should_obtain_multiple_exchange_rates_using_latest_available_range_end_interval(self):
        self.repository.store(ExchangeRate('BTC', 'USDT', BigFloat('38835.34')), 1662842954870516619)
        self.repository.store(ExchangeRate('BTC', 'USDT', BigFloat('38719.72')), 1662842975769185669)
        self.repository.store(ExchangeRate('ETH', 'USDT', BigFloat('2861.62')), 1662842954870516619)
        self.repository.store(ExchangeRate('ETH', 'USDT', BigFloat('2870.19')), 1662842975769185669)
        instrument_exchanges = [
            InstrumentExchange('BTC', 'USDT'),
            InstrumentExchange('ETH', 'USDT')
        ]
        exchange_rates = self.repository.retrieve_multiple(instrument_exchanges, range_from='-365d')
        rates_1 = exchange_rates.get_rates('BTC', 'USDT')
        self.assertEqual(len(rates_1), 2)
        self.assertEqual(rates_1[0], InstantRate(1662842975769185000, BigFloat('38719.72')))
        self.assertEqual(rates_1[1], InstantRate(1662842954870516000, BigFloat('38835.34')))
        rates_2 = exchange_rates.get_rates('ETH', 'USDT')
        self.assertEqual(len(rates_2), 2)
        self.assertEqual(rates_2[0], InstantRate(1662842975769185000, BigFloat('2870.19')))
        self.assertEqual(rates_2[1], InstantRate(1662842954870516000, BigFloat('2861.62')))


if __name__ == '__main__':
    unittest.main()
