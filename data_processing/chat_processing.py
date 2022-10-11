from curses.ascii import isdigit
from email.message import Message
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.feature_extraction.text import CountVectorizer
from stop_words import get_stop_words
from nltk.stem import WordNetLemmatizer
from feature_engineering.message_relationship import MessageRelationships
from styleformer import Styleformer
import inflect
import string
import re

class MessageTextPreprocessor():
    def __init__(self, df):
        self.df = df
        self.pipeline_order = [

        ]

    def number_to_string(self, str_):
        """ 
        translates integers as strings to their english counterpards 5->five

        for time related digits they are first detected using time_regexp and then split so that the digits are correct and the : deliminator is removed

        return pd.Series
        """
        time_regexp = r"(?:(\d{1,2}):(\d{2}))"
        time_replacement = r"\1 \2"
        return re.sub(time_regexp, time_replacement, str_)


    def preprocess(self):
        """ 
        the text preprocess pipeline
        TODO: fairly sure there is a good way to incorporate this pipeline into an sklearn friendly format
        """
        pass

    def formalise_preprocessing(self, batch_size=100, save_path = "/Users/tom/Documents/taskmaster-whatsapp-edition/model_output/stylefomer", stop_at=None):
        """
        https://github.com/PrithivirajDamodaran/Styleformer
        

        as this process is so time consuming and potentially cpu distroying it tranforms messages in batches and saves them in the save_path folder

        batch_size (int): the number of messages to formalize before saving them and continuing
        save_path (str): defaults to '/Users/tom/Documents/taskmaster-whatsapp-edition/model_output/stylefomer'. path to the folder to save messages
        """
        sf = Styleformer(style = 0)
        save_loc = save_path + "/formalized_chat.txt"
        messages = self.df.message
        create_file = True
        batch = ""
        stop_check = 0
        for i, msg in enumerate(messages):
            formal_msg = sf.transfer(msg) or msg
            batch += formal_msg + "\n"
            if i % batch_size:
                write_type = "w" if create_file else "a"
                create_file = False
                with open(save_loc, write_type) as file:
                    file.write(batch)
                batch = ""
                if stop_at != None:
                    stop_check += 1
                    if stop_check > stop_at:
                        break
                


    def number_to_word_preprocessing(self):
        """ 
        splits message by whitespace and then converts elements that are only digits to words

        only converts words that are only digits, leaving words that are partial digits untouched

        words that are only digits can include punctuation, this creates a languge problem
        . or : is used for time delimination in english
        , is used for ease of reading in english
        . is used for ease of reading in europe

        there are certainly other choices in other languages and other punc uses for english

        a digit can end the sentance but removing the . could be a mistake e.g.
        
        'the cat is 5.'
        5. contains punc which should be removed

        'the bag is 5.50'
        5.50 contains punc which shouldnt be removed
         

        return pd.Series
        """
        inflect_engine = inflect.engine()
        messages = self.df.message
        digitless_messages = []
        for msg in messages:
            words = msg.split()
            digitless_msg = ""
            for word in words:
                word_no_punc = "".join([c for c in word if c not in string.punctuation])
                if word_no_punc.isdigit():
                    word_no_punc = inflect_engine.number_to_words(word).replace(",", "")
                else:
                    word_no_punc = word
                digitless_msg += word + " "
            digitless_messages.append(digitless_msg)
        
        return pd.Series(digitless_messages)
            

        

    def token_preprocessing(self, ngram_range = (1,1), stop_words = None, messages = None, word_thresh=10):
        """ 
        tokenizes a series of messages

        ngram_range (tuple): defaults to (1,1). (min,max) of ngrams
        stop_words (str): defaults to None. available languages here-> https://github.com/Alir3z4/python-stop-words#available-languages
        messages (pd.Series): defaults to None. 
        word_thresh (int): defaults to 10. dont tokenize words that appear less often than the word_thresh
        """
        messages = messages or self.df.message
        stop_words = None if stop_words is None else get_stop_words(stop_words)

        c_vector = CountVectorizer(ngram_range = ngram_range, stop_words=stop_words, min_df=word_thresh)
        sparse_tokens = c_vector.fit_transform(messages)

        return pd.DataFrame.sparse.from_spmatrix(
            sparse_tokens,
            columns = c_vector.get_feature_names_out()
        )
    
    def token_preprocess(self):
        pass

    def segmentation_preprocess(self):
        pass

    def spell_correction_preprocess(self):
        pass

    def stemming_preprocess(self):
        pass

    def lemmatization_preprocess(self):
        pass

    def text_normalization_preprocess(self):
        pass

    def text_tagging_preprocess(self):
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
