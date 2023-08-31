import unittest
from unittest.mock import patch, mock_open
from main import DatabaseManager, FileManager, MenuManager

class TestDatabaseManager(unittest.TestCase):
    def setUp(self):
        self.db_manager = DatabaseManager(':memory:')
        self.test_phrases = ['test phrase 1', 'test phrase 2', 'test phrase 3']


class TestFileManager(unittest.TestCase):
    def setUp(self):
        self.file_manager = FileManager()

    @patch('builtins.open', new_callable=mock_open, read_data="   This is a  test   !  ")
    def test_clean_text(self, mock_file):
        cleaned_text = self.file_manager.clean_text("path/to/file.txt")
        mock_file.assert_called_once_with("path/to/file.txt", 'r', encoding='utf-8')
        self.assertEqual(cleaned_text, "This is a test")


class TestMenuManager(unittest.TestCase):
    def setUp(self):
        self.menu_manager = MenuManager()

    @patch('builtins.input', side_effect=['a'])
    def test_get_user_choice(self, mock_input):
        choice = self.menu_manager.get_user_choice()
        self.assertEqual(choice, 'a')


if __name__ == '__main__':
    unittest.main(argv=[''], exit=False)
