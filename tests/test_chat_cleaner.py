import unittest
from unittest import mock
from cleaners.chat_cleaner import RawChatCleaner
from pandas.api.types import is_datetime64_any_dtype

class TestRawChatCleaner(unittest.TestCase):
    ### load_chat_file ###

    def test_load_chat_file_empty_file_return_str_type(self):
        """ 
        returns a string when file is empty
        """
        chat_loc_data = "tests/test_data/txt_with_nothing.txt"

        cleaner = RawChatCleaner(
            chat_loc = chat_loc_data
        )

        expected = str
        output = type(cleaner.load_chat_file())

        self.assertEqual(output, expected, "expected str type to be returned")

    def test_load_chat_file_file_return_str_type(self):
        """ 
        returns a string when file contains data
        """
        chat_loc_data = "tests/test_data/txt_with_emojis.txt"

        cleaner = RawChatCleaner(
            chat_loc = chat_loc_data
        )

        expected = str
        output = type(cleaner.load_chat_file())

        self.assertEqual(output, expected, "expected str type to be returned")

    def test_load_chat_file_leaves_emojis(self):
        """ 
        leaves emojis untouched when reading
        """
        chat_loc_data = "tests/test_data/txt_with_emojis.txt"

        cleaner = RawChatCleaner(
            chat_loc = chat_loc_data
        )

        expected = "Successfully Read Chat.txt File 😬😬"
        output = cleaner.load_chat_file()

        self.assertEqual(output, expected, "expected emoji to be present in string")


    ### translate_emojis ###

    def test_translate_emojis_return_str(self):
        """
        returns string when input contains emojis or not
        """
        chat_loc_data = "tests/test_data/txt_with_nothing.txt"
        chat_data_1 = "😬😬"
        chat_data_2 = "multiple grimicing faces"

        cleaner = RawChatCleaner(
            chat_loc = chat_loc_data
        )

        expected = str
        output_1 = type(cleaner.translate_emojis(chat_data_1))
        output_2 = type(cleaner.translate_emojis(chat_data_2))

        self.assertEqual(output_1, expected, "expected str type")
        self.assertEqual(output_2, expected, "expected str type")

    ### replace_user_phone_numbers_with_names ###

    def test_replace_user_phone_numbers_with_name_return_str_type(self):
        """ 
        returns string type
        """
        chat_loc_data = "tests/test_data/txt_with_nothing.txt"
        chat_data = """ 
        my phone number is 1234
        hey @5678 are you coming?
        """

        contact_dict_data_empty = {}
        contact_dict_data_str = {
            "tom":"1234",
            "caroline":"5678"
        }
        contact_dict_data_int = {
            "tom":1234,
            "caroline":5678
        }
        cleaner_empty = RawChatCleaner(
            chat_loc = chat_loc_data,
            contact_dict = contact_dict_data_empty
        )
        cleaner_str = RawChatCleaner(
            chat_loc = chat_loc_data,
            contact_dict = contact_dict_data_str
        )
        cleaner_int = RawChatCleaner(
            chat_loc = chat_loc_data,
            contact_dict = contact_dict_data_int
        )
        
        expected = str
        output_empty = type(cleaner_empty.replace_user_phone_numbers_with_names(chat_data))
        output_str = type(cleaner_str.replace_user_phone_numbers_with_names(chat_data))
        output_int = type(cleaner_int.replace_user_phone_numbers_with_names(chat_data))

        self.assertEqual(output_empty, expected, f"expected output to be str type. instead output was: {output_empty}")
        self.assertEqual(output_str, expected, f"expected output to be str type. instead output was: {output_str}")
        self.assertEqual(output_int, expected, f"expected output to be str type. instead output was: {output_int}")

    def test_replace_user_phone_numbers_with_name_only_replace_ats(self):
        """ 
        only numbers with @<phone_num> should be replaced leaving <phone_num> untouched
        """
        
        chat_loc_data = "tests/test_data/txt_with_nothing.txt"

        chat_loc_data = "tests/test_data/txt_with_nothing.txt"
        chat_data = """ 
        my phone number is 1234
        hey @5678 are you coming?
        """

        contact_dict_data_empty = {}
        contact_dict_data_str = {
            "tom":"1234",
            "caroline":"5678"
        }
        contact_dict_data_int = {
            "tom":1234,
            "caroline":5678
        }
        cleaner_empty = RawChatCleaner(
            chat_loc = chat_loc_data,
            contact_dict = contact_dict_data_empty
        )
        cleaner_str = RawChatCleaner(
            chat_loc = chat_loc_data,
            contact_dict = contact_dict_data_str
        )
        cleaner_int = RawChatCleaner(
            chat_loc = chat_loc_data,
            contact_dict = contact_dict_data_int
        )
        
        expected_empty = """ 
        my phone number is 1234
        hey @5678 are you coming?
        """
        expected_str_int = """ 
        my phone number is 1234
        hey caroline are you coming?
        """
        output_empty = cleaner_empty.replace_user_phone_numbers_with_names(chat_data)
        output_str = cleaner_str.replace_user_phone_numbers_with_names(chat_data)
        output_int = cleaner_int.replace_user_phone_numbers_with_names(chat_data)

        self.assertEqual(output_empty, expected_empty, "expected output to be unchanged from input")
        self.assertEqual(output_str, expected_str_int, "expected only @5789 to be replaced with caroline")
        self.assertEqual(output_int, expected_str_int, "expected only @5789 to be replaced with caroline")

    ### substitute_strs ###

    def test_substitute_strs_return_str_type(self):
        """ 
        check return str type
        """
        chat_loc_data = "tests/test_data/txt_with_nothing.txt"
        chat_data = "<Media omitted> does not mean that nothing important was sent\n @1234 are you even listening?"
        contact_dict_empty = {}
        contact_dict_full = {
            "tom":"1234"
        }
        cleaner_empty = RawChatCleaner(
            chat_loc = chat_loc_data,
            contact_dict = contact_dict_empty
        )
        cleaner_full = RawChatCleaner(
            chat_loc = chat_loc_data,
            contact_dict = contact_dict_full
        )

        expected = str
        output_empty = type(cleaner_empty.substitute_strs(chat_data))
        output_full = type(cleaner_full.substitute_strs(chat_data))

        self.assertEqual(output_empty, expected, "expected output to be a str")
        self.assertEqual(output_full, expected, "expected output to be str")

    def test_substitute_strs_catch_newline(self):
        """ 
        check that correct regex matches for \n escape chars are made and replaced
        """
        chat_loc_data = "tests/test_data/txt_with_nothing.txt"
        chat_data = "\n <-remove this"

        cleaner = RawChatCleaner(
            chat_loc = chat_loc_data
        )

        expected = ".  <-remove this"
        output = cleaner.substitute_strs(chat_data)

        self.assertEqual(output, expected, f"expected only \n chars to be replaced, instead we got: {output}")

    @mock.patch.object(RawChatCleaner, "replace_user_phone_numbers_with_names")
    def test_substitute_strs_call_replace_user_phone_numbers_with_names(self, mock_replace_user_phone_numbers_with_names):
        """ 
        checks that replace_user_phone_numbers_with_names is only run when the contact_dict is populated
        """
        chat_loc_data = "tests/test_data/txt_with_nothing.txt"
        chat_data = "hello @1234 welcome to the chat, my number is 5678"
        contact_dict_empty = {}
        contact_dict_full = {
            "tom":"1234",
            "caroline":"5678"
        }

        cleaner_empty = RawChatCleaner(
            chat_loc = chat_loc_data,
            contact_dict = contact_dict_empty
        )
        cleaner_full = RawChatCleaner(
            chat_loc = chat_loc_data,
            contact_dict = contact_dict_full
        )

        cleaner_full.substitute_strs(chat_data)
        mock_replace_user_phone_numbers_with_names.assert_called()
        cleaner_empty.substitute_strs(chat_data)
        mock_replace_user_phone_numbers_with_names.assert_called_once()

    def test_substitute_strs_catch_media_omitted(self):
        """ 
        check that correct regex matches for <Media ommited> are made and replaced
        """
        chat_loc_data = "tests/test_data/txt_with_nothing.txt"
        chat_data = "<Media omitted> <media omitted> __Media_Omitted__ Media omitted l<Media omitted>dfa"

        cleaner = RawChatCleaner(
            chat_loc = chat_loc_data
        )

        expected = "__Media_Omitted__ <media omitted> __Media_Omitted__ Media omitted l__Media_Omitted__dfa"
        output = cleaner.substitute_strs(chat_data)

        self.assertEqual(output, expected, f"expected only <Media omitted> to be replaced, instead got: {output}")

    def test_str_cleaner_return_str_type(self):
        """ 
        returns str type
        tests author, message and event str_types
        """
        chat_loc_data = "tests/test_data/txt_with_nothing.txt"
        str_data = " - tom: cant believe you said that, so rude. u absolute savage"

        str_type_event = "event"
        str_type_author = "author"
        str_type_message = "message"

        cleaner = RawChatCleaner(
            chat_loc = chat_loc_data
        )

        expected = str
        output_event = type(
            cleaner.str_cleaner(
                str_data,
                str_type = "event"
            )
        )
        output_author = type(
            cleaner.str_cleaner(
                str_data,
                str_type = "author"
            )
        )
        output_message = type(
            cleaner.str_cleaner(
                str_data,
                str_type = "message"
            )
        )

        self.assertEqual(output_event, expected, "expected str output")
        self.assertEqual(output_author, expected, "expected str output")
        self.assertEqual(output_message, expected, "expected str output")
    
    def test_str_cleaner_strip(self):
        """ 
        tests that author events or author events have no leading white space or -
        """
        chat_loc_data = "tests/test_data/txt_with_nothing.txt"
        str_data_event = " - Messages and calls are end-to-end encrypted"
        str_data_author = " - Ezmay"
        str_data_message = ": ezmay could you stop ruining my unit tests?"

        cleaner = RawChatCleaner(
            chat_loc = chat_loc_data
        )

        expected_event = "Messages and calls are end-to-end encrypted"
        expected_author = "Ezmay"
        expected_message = "ezmay could you stop ruining my unit tests?"

        output_event = cleaner.str_cleaner(
            str_data_event,
            str_type = "event"
        )
        output_author = cleaner.str_cleaner(
            str_data_author,
            str_type = "author"
        )
        output_message = cleaner.str_cleaner(
            str_data_message,
            str_type = "message"
        )

        self.assertEqual(output_event, expected_event, f"expected ' - ' to be removed, instead output was: {output_event}")
        self.assertEqual(output_author, expected_author, f"expected ' - ' to be removed, instead output was: {output_author}")
        self.assertEqual(output_message, expected_message, f"expected ': ' to be removed, instead output was: {output_message}")

    def test_split_by_timestamps(self):
        """ 
        not sure this needs a test as it just calls the ubiqutous re library
        """
        pass

    def zip_timestamp_n_messages_test(self):
        """ 
        check that a list is split into two with every other element being in one of the lists
        e.g. [1,2,3,4] -> (
            [1,3],
            [2,4]
        )
        """
        chat_loc_data = "tests/test_data/txt_with_nothing.txt"
        list_data = [1,2,3,4,5,6,7,8]

        cleaner = RawChatCleaner(
            chat_loc = chat_loc_data
        )

        expected = (
            [1,3,5,7],
            [2,4,6,8]
        )
        output = cleaner.zip_timestamp_n_messages(list_data)

        self.assertEqual(output, expected, "expected lists to be divided into odds and evens")

    def is_event_test(self):
        """ 
        tests that 
        """
        chat_loc_data = "tests/test_data/txt_with_nothing.txt"
        author_data_empty = []
        author_data_full_with_author = [" - Ezmay"]
        author_data_full_with_event = ['You changed the subject from "aweful chat" to "aweful chat: the sequal you didnt want"']
        author_data_full_with_message = [': why did you make this pointless secondary chat?']
        author_data_full_with_quotation_message = ['whey you said "why did you consider this edge case" i didnt think you would go this far!']

        cleaner = RawChatCleaner(
            chat_loc = chat_loc_data
        )

        expected_empty = True
        expected_full_with_author = True
        expected_full_with_event = True
        expected_full_with_message = False
        expected_full_with_quotation_message = False

        output_empty = cleaner.is_event(author_data_empty)
        output_full_with_author = cleaner.is_event(author_data_full_with_author)
        output_full_with_event = cleaner.is_event(author_data_full_with_event)
        output_full_with_message = cleaner.is_event(author_data_full_with_message)
        output_full_with_quotation_message = cleaner.is_event(author_data_full_with_quotation_message)

        self.assertEqual(output_empty, expected_empty, "expected empty list to be True")
        self.assertEqual(output_full_with_author, expected_full_with_author, "expected list with author to be True")
        self.assertEqual(output_full_with_event, expected_full_with_event, 'expected list with first element containting a " to be True' )
        self.assertEqual(output_full_with_message, expected_full_with_message, "expected message to be False")
        self.assertEqual(output_full_with_quotation_message, expected_full_with_quotation_message, 'expected message with a " to be False')

    def test_attempt_split_message_into_author_and_content_return_tuple_of_strings(self):
        """
        tests that a tuple of two strings is returned
        """
        chat_loc_data = "tests/test_data/txt_with_nothing.txt"
        message_data = " - Ezmay: Self esteem"

        cleaner = RawChatCleaner(
            chat_loc = chat_loc_data
        )

        expected = (" - Ezmay", ": Self esteem")
        output = cleaner.attempt_split_message_into_author_and_content(message_data)
        self.assertEqual(output, expected, "expected a tuple of two strings, specifically this: (' - Ezmay', ': Self esteem)")

    def test_format_timestamp(self):
        """ 
        same as test_split_by_timestamps, this doesnt really need testing i dont think
        """
        pass

    def test_clean_data_types(self):
        """ 
        checks that data types in the matrix are correct

        timestamp (pd.datetime),
        author (str),
        is_event (bool),
        message (str)

        they are all in a pandas DataFrame object (explicitly, so not checking that aswell)


        this is an integration test but badly organised with unit testing
        """
        chat_loc_data = "tests/test_data/txt_chat_test.txt"

        cleaner = RawChatCleaner(
            chat_loc = chat_loc_data
        )

        expected = ['<M8[ns]', 'O', 'bool', 'O']
        output_chat = cleaner.clean()
        output = [output_chat[col].dtypes for col in output_chat.columns]
        self.assertEqual(output, expected, f"expected Series to be int, str, bool, datetime. Instead we got: {output}")

