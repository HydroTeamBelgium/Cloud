import os
import sys
import logging
import tempfile
import unittest
from unittest.mock import patch
from io import StringIO

from common.logger import LoggerFactory
from common.LoggerSingleton import SingletonMeta

class TestLogger(unittest.TestCase):
    
    def setUp(self):
        self.original_handlers = logging.getLogger().handlers.copy()
        self.original_env = os.environ.copy()
        
    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.original_env)
        
        root_logger = logging.getLogger()
        for handler in root_logger.handlers:
            root_logger.removeHandler(handler)
        for handler in self.original_handlers:
            root_logger.addHandler(handler)
                    
        SingletonMeta._instances.clear()
    
    def test_singleton_pattern(self):
        """Test that LoggerFactory follows the singleton pattern"""
        instance1 = LoggerFactory()
        instance2 = LoggerFactory()
        self.assertIs(instance1, instance2)
    
    def test_get_logger_basic(self):
        """Test that get_logger returns a properly configured logger"""
        logger_factory = LoggerFactory()
        logger = logger_factory.get_logger("test_logger")
        self.assertEqual(logger.name, "test_logger")
        
        self.assertGreaterEqual(len(logging.getLogger().handlers), 1)
        
        has_console_handler = False
        for handler in logging.getLogger().handlers:
            if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
                has_console_handler = True
                break
        self.assertTrue(has_console_handler)
    
    def test_console_log_level(self):
        """Test that console log level is properly set via environment variable"""
         
        SingletonMeta._instances.clear()
        
        logger_factory = LoggerFactory()
        logger_factory.set_console_log_level("WARNING")
        
        logger = logger_factory.get_logger("test_logger")
        
        console_handler = None
        for handler in logging.getLogger().handlers:
            if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
                console_handler = handler
                break
        
        self.assertIsNotNone(console_handler)
        self.assertEqual(console_handler.level, logging.WARNING)
    
    def test_invalid_console_log_level(self):
        """Test that invalid console log level defaults to INFO"""
         
        SingletonMeta._instances.clear()
        
        logger_factory = LoggerFactory()
        logger_factory.set_console_log_level("INVALID_LEVEL")
        
        logger = logger_factory.get_logger("test_logger")
        
        console_handler = None
        for handler in logging.getLogger().handlers:
            if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
                console_handler = handler
                break
        
        self.assertIsNotNone(console_handler)
        self.assertEqual(console_handler.level, logging.INFO)
    
    def test_file_logging(self):
        """Test that file logging works when LOG_FILE is set"""
        with tempfile.NamedTemporaryFile(suffix='.log') as temp_file:
            os.environ["LOG_FILE"] = temp_file.name
            
            SingletonMeta._instances.clear()
            
            logger_factory = LoggerFactory()
            logger = logger_factory.get_logger("test_logger")
            
            logger = logger_factory.get_logger("test_logger", log_file=temp_file.name)
            
            has_file_handler = False
            for handler in logger.handlers:
                if isinstance(handler, logging.FileHandler) and handler.baseFilename == os.path.abspath(temp_file.name):
                    has_file_handler = True
                    break
            
            self.assertTrue(has_file_handler)
    
    def test_file_log_level(self):
        """Test that file log level is properly set via environment variable"""
        with tempfile.NamedTemporaryFile(suffix='.log') as temp_file:
            os.environ["LOG_FILE"] = temp_file.name
            os.environ["FILE_LOG_LEVEL"] = "ERROR"

            SingletonMeta._instances.clear()
            
            logger_factory = LoggerFactory()
            logger = logger_factory.get_logger("test_logger", log_file=temp_file.name)
            
            file_handler = None
            for handler in logger.handlers:
                if isinstance(handler, logging.FileHandler) and handler.baseFilename == os.path.abspath(temp_file.name):
                    file_handler = handler
                    break
            
            self.assertIsNotNone(file_handler)
            self.assertEqual(file_handler.level, logging.ERROR)
    
    def test_specific_file_logger(self):
        """Test creating a logger with a specific log file parameter"""
        with tempfile.NamedTemporaryFile(suffix='.log') as temp_file:
            logger_factory = LoggerFactory()
            logger = logger_factory.get_logger("test_specific_logger", log_file=temp_file.name)
            
            has_file_handler = False
            for handler in logger.handlers:
                if isinstance(handler, logging.FileHandler) and handler.baseFilename == os.path.abspath(temp_file.name):
                    has_file_handler = True
                    break
            
            self.assertTrue(has_file_handler)
            self.assertFalse(logger.propagate)
            
            has_console_handler = False
            for handler in logger.handlers:
                if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
                    has_console_handler = True
                    break
            
            self.assertTrue(has_console_handler)
    
    def test_same_file_handler_not_duplicated(self):
        """Test that calling get_logger with the same file doesn't create duplicate handlers"""
        with tempfile.NamedTemporaryFile(suffix='.log') as temp_file:
            logger_factory = LoggerFactory()
            logger = logger_factory.get_logger("test_duplicate", log_file=temp_file.name)
            initial_handler_count = len(logger.handlers)
            
            logger = logger_factory.get_logger("test_duplicate", log_file=temp_file.name)
            
            self.assertEqual(len(logger.handlers), initial_handler_count)
    
    def test_dynamic_console_log_level_change(self):
        """Test changing console log level dynamically"""
        logger_factory = LoggerFactory()
        logger = logger_factory.get_logger("test_dynamic")
        
        console_handler = None
        for handler in logging.getLogger().handlers:
            if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
                console_handler = handler
                break
        
        initial_level = console_handler.level
        
        logger_factory.set_console_log_level("DEBUG")
        
        self.assertEqual(console_handler.level, logging.DEBUG)
        
        logger_factory.set_console_log_level("INFO")
        self.assertEqual(console_handler.level, logging.INFO)
    
    def test_dynamic_file_log_level_change(self):
        """Test changing file log level dynamically"""
        with tempfile.NamedTemporaryFile(suffix='.log') as temp_file:
            logger_factory = LoggerFactory()
            logger = logger_factory.get_logger("test_dynamic_file", log_file=temp_file.name)
            
            file_handler = None
            for handler in logger.handlers:
                if isinstance(handler, logging.FileHandler):
                    file_handler = handler
                    break
            
            self.assertIsNotNone(file_handler)
            
            file_handler.setLevel(logging.ERROR)
            self.assertEqual(file_handler.level, logging.ERROR)
    
    def test_log_message_formatting(self):
        """Test that log messages are properly formatted"""
        with tempfile.NamedTemporaryFile(suffix='.log') as temp_file:
            logger_factory = LoggerFactory()
            logger = logger_factory.get_logger("test_format", log_file=temp_file.name)
            
            test_message = "Test log message"
            logger.info(test_message)
            
            with open(temp_file.name, 'r') as f:
                log_content = f.read()
            
            self.assertIn("test_format", log_content)
            self.assertIn("INFO", log_content)
            self.assertIn(test_message, log_content)
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_log_to_console(self, mock_stdout):
        """Test that logging to console works"""
        with tempfile.NamedTemporaryFile(suffix='.log') as temp_file:
            os.environ["CONSOLE_LOG_LEVEL"] = "DEBUG"
            
             
            SingletonMeta._instances.clear()
            
            logger_factory = LoggerFactory()
            logger = logger_factory.get_logger("test_console", log_file=temp_file.name)
            
            test_message = "Test console message"
            logger.debug(test_message)
            
            with open(temp_file.name, 'r') as f:
                log_content = f.read()
            
            self.assertIn(test_message, log_content)

if __name__ == "__main__":
    unittest.main()
