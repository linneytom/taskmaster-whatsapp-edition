import unittest
from unittest import mock
from feature_engineering.message_relationship import MessageRelationships
import pandas as pd

class TestMessageRelationships(unittest.TestCase):

    

    @mock.patch.object(MessageRelationships, "create_feature")
    def test_build_required_features(self):
        """ 
        test that create feature is successfully called if feature doesnt exist
        """
        pass

    @mock.patch.object(MessageRelationships, "build_required_features")
    def test_create_message_group_id_build_features(self):
        """
        test that next_message_author and time_to_next_message features are created if they dont exist
        """
        pass

    def test_create_message_group_id_new_id_with_new_author(self):
        """ 
        tests that a new id is created when the author changes
        """
        pass

    def test_create_message_group_id_old_id_with_same_author(self):
        """ 
        tests that the old id is used if the author remains the same
        """
        pass

    def test_create_message_group_id_new_id_with_slow_author(self):
        """ 
        tests that a new id is created if the author remains the same but has a delay >= than the threshold
        """
        pass
