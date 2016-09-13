# Copyright 2013-2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os
import sys

from bson.timestamp import Timestamp

sys.path[0:0] = [""]

from mongo_connector.oplog_progress import OplogProgress
from mongo_connector.util import long_to_bson_ts
from tests import unittest


CHECKPOINT_FILE = "oplog.timestamp"


class TestOplogProgress(unittest.TestCase):
    """Tests for OplogProgress
    """

    def setUp(self):
        try:
            os.unlink(CHECKPOINT_FILE)
        except OSError:
            pass
        open(CHECKPOINT_FILE, "w").close()
        self.oplog_progress = OplogProgress(CHECKPOINT_FILE)

    def tearDown(self):
        try:
            os.unlink(CHECKPOINT_FILE)
        except OSError:
            pass

    def test_init(self):
        """Test init
        """
        # add a value to the file, delete the dict, and then read in the value
        self.oplog_progress.update_checkpoint("op", "rs", Timestamp(12, 34))
        self.assertEqual(self.oplog_progress.dict["rs"], Timestamp(12, 34))
        self.oplog_progress.save()

        new_oplog_progress = OplogProgress(CHECKPOINT_FILE)
        new_oplog_progress.init()

        self.assertEqual(self.oplog_progress.dict, new_oplog_progress.dict)

    def test_save(self):
        """Test save
        """
        # pretend to insert a thread/timestamp pair
        self.oplog_progress.update_checkpoint("op", "rs", Timestamp(12, 34))
        self.oplog_progress.save()

        with open(CHECKPOINT_FILE, "r") as oplog_file:
            data = json.load(oplog_file)
        self.assertEqual("rs", data[0])
        self.assertEqual(long_to_bson_ts(int(data[1])), Timestamp(12, 34))

        # ensure the temp file was deleted
        self.assertFalse(os.path.exists(CHECKPOINT_FILE + ".backup"))

    def test_update_checkpoint(self):
        """Test update checkpoint"""
        # Old oplog progress format
        self.oplog_progress.dict = {"op": Timestamp(1, 2)}
        self.oplog_progress.update_checkpoint("op", "rs", Timestamp(12, 34))

        # Old oplog progress key is removed
        self.assertNotIn("op", self.oplog_progress.dict)
        self.assertEqual(self.oplog_progress.read_checkpoint("op", "rs"),
                         Timestamp(12, 34))

        # Checkpoint of None should not overwrite the current checkpoint
        self.oplog_progress.update_checkpoint("op", "rs", None)
        self.assertEqual(self.oplog_progress.read_checkpoint("op", "rs"),
                         Timestamp(12, 34))

    def test_read_checkpoint(self):
        """Test read checkpoint"""
        self.oplog_progress.dict = {"op": Timestamp(1, 2),
                                    "rs2": Timestamp(12, 34)}
        # Old oplog progress key is readable
        self.assertEqual(self.oplog_progress.read_checkpoint("op", "rs"),
                         Timestamp(1, 2))
        # New oplog progress key is readable
        self.assertEqual(self.oplog_progress.read_checkpoint("op2", "rs2"),
                         Timestamp(12, 34))



if __name__ == "__main__":
    unittest.main()
