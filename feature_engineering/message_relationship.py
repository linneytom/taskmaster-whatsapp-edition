import pandas as pd
from datetime import timedelta
import numpy as np

class MessageRelationships():

    def __init__(self, df):
        """ 
        df is expected to be the output of RawChatCleaner.clean()
        """ 

        self.df = df
        self.features = {
            "time_since_previous_message": self.create_time_since_previous_message,
            "time_to_next_message": self.create_time_to_next_message,
            "previous_message_author": self.create_previous_message_author,
            "next_message_author": self.create_next_message_author,
            "message_group_id": self.create_message_group_id
        }
        self.future_data_leak_features = [
            "time_to_next_message",
            "next_message_author"
        ]

    def feature_exists(self, feature_name):
        return feature_name in self.df.columns
    
    def create_feature(self, feature_name):
        return self.features[feature_name]()
    
    def build_required_features(self, list_of_required_features):
        for f in list_of_required_features:
            if self.feature_exists(f):
                continue
            self.df[f] = self.create_feature(f)
    
    def create_time_since_previous_message(self):
        """ 
        return pd.Series<timedelta>
        """
        return self.df.timestamp - self.df.timestamp.shift()

    def create_time_to_next_message(self):
        """ 
        note: leaks forward information that would not be known in real time

        return pd.Series<timedelta>
        """
        return self.create_time_since_previous_message().shift(-1)

    def create_previous_message_author(self):
        """
        return pd.Series<str>
        """
        return self.df.author.shift()

    def create_next_message_author(self):
        """ 
        note: leaks forward information that would not be known in real time

        return pd.Series<str>
        """
        return self.df.author.shift(-1)
    
    def create_message_group_id(self, minute_threshold=1):
        """ 
        creates groups of messages that could be considered as the same message

        to be considered the same message it must have the same author as the next message in the chat and be within the minute_threshold

        minute_threshold (int): the number of minutes between consecutive messages of the same author that should have the same group_id

        TODO: this feature could take a while to build as it doesnt use the pd.DataFrame.apply method. may need to optimize in the future

        return pd.Series<int>
        """
        required_features = [
            "next_message_author",
            "time_to_next_message"
        ]
        self.build_required_features(required_features)

        join_bools = (
            (self.df.author==self.df.next_message_author)&
            (~self.df.is_event)&
            (self.df.time_to_next_message<=timedelta(minutes=minute_threshold))
        )

        curr_join_bools, nxt_join_bools = join_bools, np.roll(join_bools, 1)

        group_id = 0
        group_ids = []
        for curr_join, nxt_join in zip(curr_join_bools, nxt_join_bools):
            group_ids.append(group_id)
            if curr_join and nxt_join:
                continue
            if not curr_join:
                group_id += 1
        
        return pd.Series(group_ids)
