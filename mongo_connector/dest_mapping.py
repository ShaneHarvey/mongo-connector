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

import logging
import threading
import re

from collections import namedtuple

from mongo_connector import errors


LOG = logging.getLogger(__name__)

"""New structure to handle both plain and
wildcard mapping namespaces dynamically.
"""


_MappedNamespace = namedtuple('MappedNamespace',
                              ['name', 'include_fields', 'exclude_fields'])


class MappedNamespace(_MappedNamespace):
    def __new__(cls, name=None, include_fields=None, exclude_fields=None):
        return super(MappedNamespace, cls).__new__(
            cls, name, include_fields, exclude_fields)


class DestMapping(object):
    def __init__(self, namespace_set=None, ex_namespace_set=None,
                 user_mapping=None, include_fields=None,
                 exclude_fields=None):
        """
            namespace_set and ex_namespace_set will not be non-empty
            in the same time.
            user_mapping should be non-empty together with namespace_set.
            This assumption has been verified in the get_config_options()
            in connector.py.
        """
        # a dict containing plain mappings
        self.plain = {}
        # a dict containing wildcard mappings
        self.wildcard = {}
        # a dict containing reverse plain mapping
        # because the mappings are not duplicated,
        # so the values should also be unique
        self.reverse_plain = {}
        # a dict containing plain db mappings, db -> a set of mapped db
        self.plain_db = {}

        # Fields to include or exclude from all namespaces
        self.include_fields = include_fields
        self.exclude_fields = exclude_fields

        # the input namespace_set and ex_namespace_set could contain wildcard
        self.namespace_set = set()
        if ex_namespace_set:
            self.ex_namespace_set = set(ex_namespace_set)
        else:
            self.ex_namespace_set = set()

        self.lock = threading.Lock()

        user_mapping = user_mapping or {}
        namespace_set = namespace_set or []
        # initialize
        for ns in namespace_set:
            user_mapping.setdefault(ns, ns)

        for src_name, v in user_mapping.items():
            if isinstance(v, dict):
                self._add_mapping(src_name, v.get('rename'), v.get('fields'),
                                  v.get('excludeFields'))
            else:
                self._add_mapping(src_name, v)

    def _add_mapping(self, src_name, dest_name=None, include_fields=None,
                     exclude_fields=None):
        if (self.include_fields and exclude_fields or
                self.exclude_fields and include_fields or
                include_fields and exclude_fields):
            raise errors.InvalidConfiguration(
                "Cannot mix include fields and exclude fields in "
                "namespace mapping for: '%s'" % (src_name,))
        if dest_name is None:
            dest_name = src_name
        self.set(src_name, MappedNamespace(dest_name, include_fields,
                                           exclude_fields))
        # Add the namespace for commands on this database
        cmd_name = src_name.split('.', 1)[0] + '.$cmd'
        dest_cmd_name = dest_name.split('.', 1)[0] + '.$cmd'
        self.set(cmd_name, MappedNamespace(dest_cmd_name))

    def set_plain(self, src_name, mapped_namespace):
        """A utility function to set the corresponding plain variables"""
        target_name = mapped_namespace.name
        existing_src = self.reverse_plain.get(target_name)
        if existing_src and existing_src != src_name:
            raise errors.InvalidConfiguration(
                "Multiple namespaces cannot be combined into one target "
                "namespace. Trying to map '%s' to '%s' but there already "
                "exists a mapping from '%s' to '%s'" %
                (src_name, target_name, existing_src, target_name))

        self.plain[src_name] = mapped_namespace
        self.reverse_plain[target_name] = src_name
        src_db, src_col = src_name.split(".", 1)
        if src_col != "$cmd":
            target_db = target_name.split(".")[0]
            self.plain_db.setdefault(src_db, set()).add(target_db)

    def match(self, src, wildcard_ns):
        """If source string src matches dst, return the matchobject"""
        if wildcard_ns.find('*') < wildcard_ns.find('.'):
            # A database name cannot contain a '.' character
            wildcard_group = '([^.]*)'
        else:
            wildcard_group = '(.*)'
        reg_pattern = r'\A' + wildcard_ns.replace('*', wildcard_group) + r'\Z'
        return re.match(reg_pattern, src)

    def match_set(self, plain_src, dst_arr):
        for x in dst_arr:
            if plain_src == x:
                return True
            if "*" in x:
                m = self.match(plain_src, x)
                if m:
                    return True

        return False

    def replace(self, src_match, map_pattern):
        """Given the matchobject src_match,
        replace corresponding '*' in map_pattern with src_match."""
        wildcard_matched = src_match.group(1)
        return map_pattern.replace("*", wildcard_matched)

    def get(self, plain_src_ns):
        """Given a plain source namespace, return a mapped namespace if it
        should be included or None.
        """
        # if plain_src_ns matches ex_namespace_set, ignore
        if self.match_set(plain_src_ns, self.ex_namespace_set):
            return None
        if not self.wildcard and not self.plain:
            # here we include all namespaces
            return MappedNamespace(plain_src_ns)
        with self.lock:
            # search in plain mappings first
            try:
                return self.plain[plain_src_ns]
            except KeyError:
                # search in wildcard mappings
                # if matched, get a replaced mapped namespace
                # and add to the plain mappings
                for wildcard_name, mapped in self.wildcard.items():
                    match = self.match(plain_src_ns, wildcard_name)
                    if not match:
                        continue
                    new_name = self.replace(match, mapped.name)
                    new_mapped = MappedNamespace(new_name,
                                                 mapped.include_fields,
                                                 mapped.exclude_fields)
                    self.set_plain(plain_src_ns, new_mapped)
                    return new_mapped

            return None

    def set(self, src_name, mapped_namespace):
        """Add a new namespace mapping."""
        with self.lock:
            if "*" in src_name:
                self.wildcard[src_name] = mapped_namespace
            else:
                self.set_plain(src_name, mapped_namespace)
        self.namespace_set.add(src_name)

    def unmap_namespace(self, plain_mapped_ns):
        """Given a plain mapped namespace, return a source namespace if
        matched. It is possible for the mapped namespace to not yet be present
        in the plain/reverse_plain dictionaries so we search the wildcard
        dictionary as well.
        """
        if not self.wildcard and not self.plain:
            return plain_mapped_ns

        src_name = self.reverse_plain.get(plain_mapped_ns)
        if src_name:
            return src_name
        for wildcard_src_name, mapped in self.wildcard.items():
            match = self.match(plain_mapped_ns, mapped.name)
            if not match:
                continue
            return self.replace(match, wildcard_src_name)
        return None

    def map_namespace(self, plain_src_ns):
        """Applies the plain source namespace mapping to a "db.collection" string.
        The input parameter ns is plain text.
        """
        mapped = self.get(plain_src_ns)
        if mapped:
            return mapped.name
        else:
            return None

    def map_db(self, plain_src_db):
        """Applies the namespace mapping to a database.
        Individual collections in a database can be mapped to
        different target databases, so map_db can return multiple results.
        The input parameter db is plain text.
        This is used to dropDatabase, so we assume before drop, those target
        databases should exist and already been put to plain_db when doing
        create/insert operation.
        """
        if not self.wildcard and not self.plain:
            return [plain_src_db]
        # Lookup this namespace to seed the plain_db dictionary
        self.get(plain_src_db + '.$cmd')
        return list(self.plain_db.get(plain_src_db, set()))

    def fields(self, plain_src_ns):
        """Get the fields to include and exclude for a given namespace."""
        mapped = self.get(plain_src_ns)
        if mapped:
            return mapped.include_fields, mapped.exclude_fields
        else:
            return None, None

    def projection(self, plain_src_name, projection):
        """For the given source namespace return the projected fields."""
        include_fields, exclude_fields = self.fields(plain_src_name)
        fields = include_fields or exclude_fields
        include = 1 if include_fields else 0
        if fields:
            full_projection = dict((field, include) for field in fields)
            if projection:
                full_projection.update(projection)
            return full_projection
        return projection
