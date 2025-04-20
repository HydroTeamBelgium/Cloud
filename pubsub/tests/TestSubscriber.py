
from logging import Logger
from common.config import load_config
import unittest
from common.logger import LoggerFactory
tmp = load_config()

class TestSubscriber(unittest.TestCase):

    _logger: Logger
    
    def setUp(self):
        self._logger = LoggerFactory.get_logger(__name__, "subscriber_test_logs")
        
    def tearDown(self):
        pass
    
    def test_config_retrieval(self):
        tmp = load_config()
        self.assertIsNotNone(tmp.get("subscriber"))


if __name__ == "__main__":
    unittest.main()
