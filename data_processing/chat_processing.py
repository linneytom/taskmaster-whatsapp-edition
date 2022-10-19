import pandas as pd
from datetime import datetime
from stop_words import get_stop_words
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet
from feature_engineering.message_relationship import MessageRelationships

class MessageTextPreprocessor():
    def __init__(self, df):
        self.df = df
        self.pipeline_order = [

        ]
    
    def lemmatization(self):
        pass

class ChatDataProcessor():

    def __init__(self, cleaned_chat):
        """ 
        takes the output of RawChatCleaner to process into various datasets ready for further text cleaning or timeseries analysis

        cleaned_chat (pd.DataFrame): the output of RawChatCleaner().clean()
        """
        self.cleaned_chat = cleaned_chat

    def group_messages(self):
        """ 
        groups consecutive messages using the message_group_id
        """
        feature_engine = MessageRelationships(self.cleaned_chat)
        message_groups = feature_engine.create_feature("message_group_id")
        grouped_chat = self.cleaned_chat.groupby(
            message_groups
        ).agg(
            {
                "message":"sum",
                "timestamp":"first",
                "author":"first",
                "is_event":"first",
            }
        )
        return grouped_chat


    def create_full_ts_range(self, freq):
        """ 
        creates rows for days|hours where no events or messages exist

        freq (str): fills missing hours if 'h' else fills missing days

        return pd.MultiIndex
        """

        # first we create a table informing us on what the min, max of each author is over the requested freq
        if freq == "h":
            # we create this author_ranges var for hours just so that everything is semetrical and no vars are called without being created first
            author_ranges = pd.DataFrame(
                [[a,0,23] for a in self.cleaned_chat.author.unique()],
                columns = ["author","freq_min", "freq_max"]
            ).set_index("author")
            
            range_creator = lambda x: pd.date_range(
                datetime.today().replace(
                    hour=0, 
                    minute=0,
                    second=0, 
                    microsecond=0
                ),
                periods=24,
                freq="h"
            )
        else:
            author_ranges = self.cleaned_chat.groupby("author").agg(
                {
                    "timestamp":["min","max"]
                }
            ).apply(lambda x: x.dt.date)
            author_ranges.columns = ["freq_min", "freq_max"]
            range_creator = lambda x: pd.date_range(x["freq_min"], x["freq_max"], freq="d")
        
        # now for each author we reindex to fill in missing freq, we add a synthetic_row column to indicate which rows have been created during this process

        exploded_range = author_ranges.apply(
            range_creator, 
            axis = 1
        ).explode().to_frame().reset_index()
        exploded_range.columns = ["author","timestamp"]
        full_index_range = self.group_by_ts_freq(
            freq = freq,
            df_to_index=exploded_range
        ).index

        return full_index_range
    
    def group_by_ts_freq(self, freq, agg={}, df_to_index=None):
        """ 
        creates a timeseries using the freq the author is present in the data

        freq (str): the frequency of the timeseries data, supports h, d, w, m, y
        agg (dict): keys are the column name after aggregating, values are lambda functions
        df_to_index (pd.DataFrame): a dataframe to group over ts freq, just aggregates using a 'count'. this is used to group the full range over the same frequency as the cleaned_chat

        return pd.DataFrame
        """
        groupby_freq = {
            "h": lambda x: [
                    x.author, 
                    x.timestamp.dt.hour.rename("hour")
                ],
            "d": lambda x: [
                    x.author, 
                    x.timestamp.dt.date.rename("date")
                ],
            "w": lambda x: [
                    x.author, 
                    x.timestamp.dt.year.rename("year"), 
                    x.timestamp.dt.isocalendar().week.rename("isoweek")
                ],
            "m": lambda x: [
                    x.author, 
                    x.timestamp.dt.year.rename("year"), 
                    x.timestamp.dt.month.rename("month")
                ],
            "y": lambda x: [
                    x.author, 
                    x.timestamp.dt.year.rename("year")
                ]
        }

        if df_to_index is None:
            return self.cleaned_chat.groupby(
                groupby_freq[freq](self.cleaned_chat)
            ).apply(self.ts_agg, agg = agg)
        return df_to_index.groupby(
                groupby_freq[freq](df_to_index)
            )['timestamp'].count()
        
    
    def ts_agg(self, frame, agg={}):
        """ 
        runs any requested aggregations along with some default ones

        agg (dict): keys are the column name after aggregating, values are lambda functions

        returns pd.Series
        """
        aggregated = {}
        for col, agg_lambda in agg.items():
            aggregated[col] = agg_lambda(frame)
        
        aggregated["event_count"] = frame["is_event"].sum()
        aggregated["message_count"] = frame["message"].count() - aggregated["event_count"]

        return pd.Series(aggregated)

    def make_timeseries(self, freq, agg={}):
        """ 
        converts chat data into a timeseries grouped over authors and a frequency

        freq (str): the frequency of the timeseries data, supports h, d, w, m, y
        agg (dict): keys are the column name after aggregating, values are lambda functions

        return pd.DataFrame
        """
        incomplete_ts = self.group_by_ts_freq(freq=freq, agg=agg)
        complete_ts_range = self.create_full_ts_range(freq=freq)

        # create an empty dataframe with the full time range
        complete_ts = pd.DataFrame(
            [[]]*len(complete_ts_range), 
            index=complete_ts_range
        )

        # create the full timeseries
        complete_ts = complete_ts.join(incomplete_ts)
        complete_ts["synthetic_row"] = complete_ts.message_count.isnull()

        #fill nulls
        complete_ts = complete_ts.fillna(0)
        return complete_ts
