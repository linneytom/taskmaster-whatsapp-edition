import re
from datetime import datetime
import pandas as pd
import emoji


class RawChatCleaner():

    def __init__(self, chat_loc, contact_dict = {}):
        """ 
        cleaner for the raw chat.txt exported from whatsapp

        chat_loc (str): the absolute file path of the whatsapp chat.txt file

        the clean() method 

        TODO: remove media ommited messages and other events
        """
        self.chat_loc = chat_loc
        self.contact_dict = contact_dict
        self.chat_with_emojis = self.load_chat_file()
        self.chat = self.translate_emojis(self.chat_with_emojis)

        # TODO: need to check this, i think it should be \d{2} rather than \d{0,9}
        self.timestamp_regexp = re.compile(
            r"(\d{0,9}\/\d{0,9}\/\d{0,9},\s\d{0,9}:\d{0,9})"
        )
        self.author_regexp = re.compile(
            r"(^.*?(?=\:))"
        )
        self.quotation_regexp = re.compile(
            r"\""
        )
        self.media_ommited_regexp = re.compile(
            r"(?:<Media omitted>)"
        )
        self.newline_regexp = re.compile(
            r"(?:\n)" # not sure why we dont need an additional \ here
        )

    def load_chat_file(self):
        """ 
        loads the chat

        return str
        """
        return open(self.chat_loc, "r").read()

    def translate_emojis(self, encoded_chat):
        """ 
        transforms emojis into text

        encoded_chat (st): chat

        return str
        """
        translated_emoji_chat = emoji.demojize(
            encoded_chat,
            delimiters = (" :", ": ")
        )
        return translated_emoji_chat
    
    def replace_user_phone_numbers_with_names(self, str_):
        """ 
        uses a regexp to locate and replace the number of an chat member with their name. this data needs to be supplied by the user of the program as there is no reliable datasource for linking these user descriptors

        contact_dict (dict): name<str>: phone_number<str|int>
        """

        for name, number in self.contact_dict.items():
            regexp = re.compile(f"(?:@{number})")
            if regexp.search(str_):
                str_ = regexp.sub(name, str_)
        return str_

    def substitute_strs(self, str_):
        """ 
        replaces some txt elements with easier tokenizable versions or with the contact_dict

        this has to be done after most of the cleaning as the \n char can be used for splitting messages
        """
        str_ = self.media_ommited_regexp.sub(
            "__Media_Omitted__",
            str_
        )
        str_ = self.newline_regexp.sub(
            ". ",
            str_
        )
        if len(self.contact_dict) > 0:
            str_ = self.replace_user_phone_numbers_with_names(str_)
        return str_
    
    def clean(self):
        """ 
        returns a dataframe of the chat data with the following columns

        timestamp (datetime), author (str), is_event (bool), message (str), time_since_previous_message (timedelta), previous_message_author (str)

        return pd.DataFrame
        """
        splitted_chat = self.split_by_timestamps()
        chat_matrix = []
        for ts, raw_msg in self.zip_timestamp_n_messages(splitted_chat):
            author, msg = self.attempt_split_message_into_author_and_content(raw_msg)
            chat_matrix.append(
                [
                    self.format_timestamp(ts),
                    self.str_cleaner(author, "author"),
                    author=="",
                    self.str_cleaner(msg, "event" if author=="" else "message")
                ]
            )
        chat_data = pd.DataFrame(
            chat_matrix,
            columns = [
                "timestamp",
                "author",
                "is_event",
                "message"
            ]
        )

        return chat_data
    
    def str_cleaner(self, str_, str_type):
        """ 
        removes the leading ' - ' from the author
        """
        if (str_type == "event") or (str_type == "author"):
            return self.substitute_strs(str_[3:])
        elif str_type == "message":
            return self.substitute_strs(str_[2:])
    
    def split_by_timestamps(self):
        """ 
        splits the chat into a list of timestamps and messages 
        
        e.g. [timestamp1, message1, timestamp2, message2, ..., timestampN, messageN]

        return list<str>
        """
        return self.timestamp_regexp.split(self.chat)[1:] # remove first element to drop empty string


    def zip_timestamp_n_messages(self, splitted_chat):
        """ 
        zips together timestamps with their respective message
        e.g. (
            [timestamp1, ..., timestampN],
            [message1, ..., messageN]
        )

        splitted_chat (list<str>): a list representing the chat after it has been split by the timestamp_regexp

        return tuple<list<str>>
        """
        timestamps = splitted_chat[::2]
        messages = splitted_chat[1::2]
        return zip(timestamps, messages)

    def is_event(self, author_data):
        """
        if no split can be found then the message is an event and has no author

        uses the quotation_regexp to differentiate between authors and events that contain a ': '

        author_data (list<str>): a list of strings from the author_data that match the author_regexp

        return bool (True if message is an event)
        """
        if (len(author_data) == 0) or (self.quotation_regexp.search(author_data[0])):
            return True
        return False


    def attempt_split_message_into_author_and_content(self, message):
        """ 
        splits the message into an author and its contents if possible. uses the is_event to return an empty author value along with the original message if message is an event

        message (str): an individual message containing potential author metadata

        return str, str (author, message)
        """
        author_data = self.author_regexp.findall(message)

        if self.is_event(author_data):
            return "", message
        
        author = author_data[0]
        message = ''.join(self.author_regexp.split(message)[2:]) # drop the author and empty string before joining the rest as a single message

        return author, message

    def format_timestamp(self, ts, date_format = "%d/%m/%Y, %H:%M"):
        """ 
        formats a timestamp str into a datetime object

        ts (str): the timestamp string to format
        date_format (str): the format of the date, defaults to %d/%m/%Y, %H:%M

        return datetime
        """
        try:
            return datetime.strptime(ts, date_format)
        except:
            return datetime.datetime(
                day = 1,
                month = 1,
                year = 1900, 
                hour = 0, 
                minute = 0
            )