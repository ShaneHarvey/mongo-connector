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

"""Tests methods for mongo_connector
"""

import json
import os
import sys
import time

from bson.timestamp import Timestamp

sys.path[0:0] = [""]

from mongo_connector.connector import Connector
from mongo_connector.test_utils import ReplicaSet, connector_opts
from tests import unittest


class TestMongoConnector(unittest.TestCase):
    """ Test Class for the Mongo Connector
    """

    @classmethod
    def setUpClass(cls):
        """ Initializes the cluster
        """
        try:
            os.unlink("oplog.timestamp")
        except OSError:
            pass
        cls.repl_set = ReplicaSet().start()

    @classmethod
    def tearDownClass(cls):
        """ Kills cluster instance
        """
        cls.repl_set.stop()

    def test_connector(self):
        """Test whether the connector initiates properly
        """
        conn = Connector(
            mongo_address=self.repl_set.uri,
            ns_set=['test.test'],
            **connector_opts
        )
        conn.start()

        while len(conn.shard_set) != 1:
            time.sleep(2)
        conn.join()

        self.assertFalse(conn.can_run)
        time.sleep(5)
        for thread in conn.shard_set.values():
            self.assertFalse(thread.running)


if __name__ == '__main__':
    unittest.main()
