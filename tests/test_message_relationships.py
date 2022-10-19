import time
import unittest
from unittest import mock
from feature_engineering.message_relationship import MessageRelationships
import pandas as pd

def read_csv_with_timestamps(
    df_loc, 
    timestamp_cols=["timestamp"],
    timedelta_cols=[]
    ):
    df = pd.read_csv(df_loc)
    for col in timestamp_cols:
        df[col] = pd.to_datetime(df[col])
    for col in timedelta_cols:
        df[col] = pd.to_timedelta(df[col])
    
    return df

def spy_on(instance, method_name):
    method = getattr(instance, method_name)
    spy = mock.patch.object(instance, method_name, wraps = method)
    return spy.start()

class TestMessageRelationships(unittest.TestCase):

    def test_feature_creators_named_correctly(self):
        """ 
        tests that all feature engines have the correct naming convention and spelling is the same between the func and the feature name
        """
        df_data = read_csv_with_timestamps(
            "tests/test_data/clean_chat_test.csv"
        )
        feature_engine = MessageRelationships(df_data)
        expected_feature_creators = [
            "create_" + feature for feature in feature_engine.features.keys()
        ]
        class_methods = dir(feature_engine)
        for feature in expected_feature_creators:
            self.assertIn(feature, class_methods, "expected feature create func not a MessageRelationships method")
    
    def test_create_feature_is_correct_method(self):
        """ 
        tests that the feature name calls the correct create_ method
        """
        df_data = read_csv_with_timestamps(
            "tests/test_data/clean_chat_test.csv"
        )
        feature_engine = MessageRelationships(df_data)
        features = feature_engine.features
        for name, method in features.items():
            expected_method = "create_" + name
            output_method = method.__name__
            self.assertEquals(
                expected_method,
                output_method,
                f"incorrect method was called for feature: {name}"
            )

    def test_create_message_group_id_build_features(self):
        """
        test that next_message_author and time_to_next_message features are created if they dont exist
        """
        
        df_data_without_features = read_csv_with_timestamps(
            "tests/test_data/clean_chat_test.csv"
        )
        df_data_with_features = read_csv_with_timestamps(
            "tests/test_data/clean_chat_test_with_group_preprocessing.csv",
            timedelta_cols = ["time_to_next_message"]
        )

        feature_engine_1 = MessageRelationships(
            df_data_without_features
        )
        feature_engine_2 = MessageRelationships(
            df_data_with_features
        )
        expected_call_args_1 = sorted(["next_message_author","time_to_next_message"])
        expected_call_count_2 = 0

        spy_1 = spy_on(feature_engine_1, "create_feature")
        feature_engine_1.create_message_group_id()
        output_call_args_1 = sorted([
            call.args[0] for call in spy_1.call_args_list
        ])
        self.assertEquals(output_call_args_1, expected_call_args_1, f"expected create_feature to call 'next_message_author' and 'time_to_next_message'. instead called {output_call_args_1}")

        spy_2 = spy_on(feature_engine_2, "create_feature")
        feature_engine_2.create_message_group_id()
        output_call_2 = spy_2.call_count
        self.assertEquals(output_call_2, expected_call_count_2, f"create_feature should have been called twice one for the two missing features. create_feature was called {output_call_2} times")

    def test_create_message_group_id_identifies_groups(self):
        """ 
        tests that consecutive messages from the same author have the same group if appropriate
        """
        # should group the messages of the same author
        df_data_to_group = read_csv_with_timestamps(
            "tests/test_data/clean_chat_to_group.csv"
        )
        to_group_engine = MessageRelationships(df_data_to_group)
        # should not group events
        df_data_event_group = read_csv_with_timestamps(
            "tests/test_data/clean_chat_event_group.csv"
        )
        event_group_engine = MessageRelationships(df_data_event_group)
        # should not group quick conversations between two authors
        df_data_overlapping_groups = read_csv_with_timestamps(
            "tests/test_data/clean_chat_overlapping_groups.csv"
        )
        overlapping_groups_engine = MessageRelationships(df_data_overlapping_groups)
        # should create two groups seperated by an event
        df_data_interupted_group = read_csv_with_timestamps(
            "tests/test_data/clean_chat_interupted_group.csv"
        )
        interupted_group_engine = MessageRelationships(df_data_interupted_group)


        expected_to_group = pd.Series([0,0,0])
        output_to_group = to_group_engine.create_message_group_id()
        pd.testing.assert_series_equal(output_to_group, expected_to_group)

        expected_event_group = pd.Series([0,1,2])
        output_event_group = event_group_engine.create_message_group_id()
        pd.testing.assert_series_equal(output_event_group, expected_event_group)

        expected_overlapping_groups = pd.Series([0,1,2,3,4,5])
        output_overlapping_groups = overlapping_groups_engine.create_message_group_id()
        pd.testing.assert_series_equal(output_overlapping_groups, expected_overlapping_groups)

        expected_interupted_group = pd.Series([0,1,1,2,3,4])
        output_interupted_group = interupted_group_engine.create_message_group_id()
        pd.testing.assert_series_equal(output_interupted_group, expected_interupted_group)

    def test_message_relationships_are_series_object(self):
        """ 
        test that features created by MessageRelationships are Series objects
        """
        df_data = read_csv_with_timestamps("tests/test_data/clean_chat_test.csv")
        engine = MessageRelationships(df_data)
        expected_type = pd.Series
        for feature_name, create_feature in engine.features.items():
            output_type = type(
                create_feature()
            )
            self.assertEquals(output_type, expected_type, f"expected pd.Series type for feature: {feature_name}. Instead is {output_type} type")
