
from logging import Logger

import unittest
from common.logger import LoggerFactory
from pubsub.subscribers.Subscriber import Subscriber


class TestConfig(unittest.TestCase):

    _logger: Logger
    
    def setUp(self):
        self._logger = LoggerFactory.get_logger(__name__, "subscriber_test_logs")
        
    def tearDown(self):
        pass
    
    #TODO: give github pipeline credentials to for gcp
    """
    def test_config_retrieval(self):
        tmp = Subscriber("haha", "lol", "ding") #should be changed to relevant parameters
        self.assertIsNotNone(tmp._pubsub_config.get("subscriber"))
    """
    


if __name__ == "__main__":
    unittest.main()
