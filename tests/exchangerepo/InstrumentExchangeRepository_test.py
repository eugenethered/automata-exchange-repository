import unittest

from cache.holder.RedisCacheHolder import RedisCacheHolder
from core.exchange.InstrumentExchange import InstrumentExchange
from exchange.InstrumentExchangesHolder import InstrumentExchangesHolder

from exchangerepo.repository.InstrumentExchangeRepository import InstrumentExchangeRepository


class InstrumentExchangeRepositoryTestCase(unittest.TestCase):

    def setUp(self) -> None:
        options = {
            'REDIS_SERVER_ADDRESS': '192.168.1.90',
            'REDIS_SERVER_PORT': 6379,
            'INSTRUMENT_EXCHANGES_KEY': 'test:instrument-exchanges'
        }
        self.cache = RedisCacheHolder(options)
        self.repository = InstrumentExchangeRepository(options)

    def tearDown(self):
        self.cache.delete_timeseries('test:instrument-exchanges')

    def test_should_store_instrument_exchanges_via_repo(self):
        instrument_exchanges = InstrumentExchangesHolder()
        instrument_exchanges.add(InstrumentExchange('OTC', 'GBP'))
        instrument_exchanges.add(InstrumentExchange('OTC', 'USDT'))
        self.repository.store(instrument_exchanges)
        stored_instrument_exchanges = self.repository.retrieve()
        self.assertEqual(len(stored_instrument_exchanges.get_all()), 2)

    def test_should_append_store_instrument(self):
        instrument_exchanges = InstrumentExchangesHolder()
        instrument_exchanges.add(InstrumentExchange('OTC', 'GBP'))
        instrument_exchanges.add(InstrumentExchange('OTC', 'USDT'))
        self.repository.store(instrument_exchanges)
        self.repository.append_store(InstrumentExchange('OTC', 'BTC'))
        stored_instrument_exchanges = self.repository.retrieve()
        self.assertEqual(len(stored_instrument_exchanges.get_all()), 3)


if __name__ == '__main__':
    unittest.main()
