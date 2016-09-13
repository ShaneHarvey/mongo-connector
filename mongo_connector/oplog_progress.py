# Copyright 2016 MongoDB, Inc.
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
import logging
import os
import shutil
import threading

from mongo_connector import util


LOG = logging.getLogger(__name__)


class OplogProgress(object):
    """Maintains the last stable state of mongo-connector.

    One instance of this class is shared between the main connector thread and
    each OplogThread. It is also serialized to disk as the oplog progress
    file.
    """

    def __init__(self, oplog_progress_file=None):
        self.oplog_progress_file = oplog_progress_file
        self.dict = {}
        self.lock = threading.Lock()

    def init(self):
        """Reads oplog progress from the oplog progress file.
        This method is only called once before any threads are spawned.
        """
        if self.oplog_progress_file is None:
            return

        # Check for empty file
        try:
            if os.stat(self.oplog_progress_file).st_size == 0:
                LOG.info("OplogProgress: Empty oplog progress file.")
                return
        except OSError:
            return

        with open(self.oplog_progress_file, 'r') as progress_file:
            try:
                data = json.load(progress_file)
            except ValueError:
                LOG.exception(
                    'Cannot read oplog progress file "%s". '
                    'It may be corrupt after Mongo Connector was shut down'
                    'uncleanly. You can try to recover from a backup file '
                    '(may be called "%s.backup") or create a new progress file '
                    'starting at the current moment in time by running '
                    'mongo-connector --no-dump <other options>. '
                    'You may also be trying to read an oplog progress file '
                    'created with the old format for sharded clusters. '
                    'See https://github.com/10gen-labs/mongo-connector/wiki'
                    '/Oplog-Progress-File for complete documentation.',
                    self.oplog_progress_file, self.oplog_progress_file)
                return
            # data format:
            # [name, timestamp] = replica set
            # [[name, timestamp], [name, timestamp], ...] = sharded cluster
            if not isinstance(data[0], list):
                data = [data]
            for name, timestamp in data:
                self.dict[name] = util.long_to_bson_ts(timestamp)

    def save(self):
        """Writes oplog progress to the oplog progress file.
        """
        if self.oplog_progress_file is None:
            return

        with self.lock:
            items = [[name, util.bson_ts_to_long(self.dict[name])]
                     for name in self.dict]
            if not items:
                return

        # write to temp file
        backup_file = self.oplog_progress_file + '.backup'
        os.rename(self.oplog_progress_file, backup_file)

        with open(self.oplog_progress_file, 'w') as dest:
            if len(items) == 1:
                # Write 1-dimensional array, as in previous versions.
                json_str = json.dumps(items[0])
            else:
                # Write a 2d array to support sharded clusters.
                json_str = json.dumps(items)
            try:
                dest.write(json_str)
            except IOError:
                # Basically wipe the file, copy from backup
                dest.truncate()
                with open(backup_file, 'r') as backup:
                    shutil.copyfile(backup, dest)

        os.remove(backup_file)

    def update_checkpoint(self, oplog_col, replset_name, checkpoint):
        """Store the current checkpoint in the oplog progress dictionary.
        """
        if checkpoint is None:
            LOG.debug("OplogProgress: no checkpoint to update.")
            return

        # update the oplog checkpoint for the specified replica set
        with self.lock:
            # If we have the repr of our oplog collection in the dictionary,
            # remove it and replace it with our replica set name.
            # This allows an easy upgrade path from mongo-connector 2.3.
            # For an explanation of the format change, see the comment in
            # read_checkpoint.
            self.dict.pop(str(oplog_col), None)
            self.dict[replset_name] = checkpoint
            LOG.debug("OplogProgress: oplog checkpoint updated to %s" %
                      str(checkpoint))

    def read_checkpoint(self, oplog_col, replset_name):
        """Read the last checkpoint from the oplog progress dictionary.
        """
        # In versions of mongo-connector 2.3 and before, we used the repr of the
        # oplog collection as keys in the oplog_progress dictionary.
        # In versions thereafter, we use the replica set name. For backwards
        # compatibility, we check for both.
        oplog_str = str(oplog_col)

        ret_val = None
        with self.lock:
            try:
                # New format.
                ret_val = self.dict[replset_name]
            except KeyError:
                try:
                    # Old format.
                    ret_val = self.dict[oplog_str]
                except KeyError:
                    pass

        LOG.debug("OplogProgress: reading last checkpoint as %s " %
                  str(ret_val))
        return ret_val
