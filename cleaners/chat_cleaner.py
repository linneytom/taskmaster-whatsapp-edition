import re
from datetime import datetime


class RawChatCleaner():

    def __init__(self, chat_loc):
        """ 
        cleaner for the raw chat.txt exported from whatsapp

        chat_loc (str): the absolute file path of the whatsapp chat.txt file

        the clean() method 

        TODO: remove media ommited messages and other events
        """
        self.chat_loc = chat_loc
        self.raw_chat = open(self.chat_loc, "r").read()
        self.chat = self.decode_chat(self.raw_chat)
        self.timestamp_regexp = re.compile(
            r"(\d{0,9}\/\d{0,9}\/\d{0,9},\s\d{0,9}:\d{0,9})"
        )
        self.author_regexp = re.compile(
            r"(^.*?(?=\:))"
        )
        self.quotation_regexp = re.compile(
            r"\""
        )
    
    def clean(self):
        """ 
        returns a matrix of the chat data with the following columns

        timestamp (datetime), author (str), is_event (bool), message (str)

        return list<list>
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
        return chat_matrix
    
    def str_cleaner(self, str_, str_type):
        """ 
        removes the leading ' - ' from the author
        """
        if (str_type == "event") or (str_type == "author"):
            return str_[3:]
        elif str_type == "message":
            return str_[2:]

    def decode_chat(self, chat):
        """ 
        removes unicode characters from the chat

        chat (str): unprocessed chat string

        returns str
        """
        chat_unicoded = chat.encode("ascii", "ignore")
        return chat_unicoded.decode()
    
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

        yield tuple<list<str>>
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