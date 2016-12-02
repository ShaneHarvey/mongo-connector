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

import re

from tests import unittest
from mongo_connector.dest_mapping import (
    DestMapping, MappedNamespace, match_replace_regex, namespace_to_regex,
    RegexSet)
from mongo_connector import errors


class TestDestMapping(unittest.TestCase):
    """Test the DestMapping class"""

    def test_default(self):
        """Test that by default, all namespaces are kept without renaming"""
        dest_mapping = DestMapping()
        self.assertEqual(dest_mapping.unmap_namespace("db1.col1"), "db1.col1")
        self.assertEqual(dest_mapping.map_db("db1"), ["db1"])
        self.assertEqual(dest_mapping.map_namespace("db1.col1"), "db1.col1")

    def test_include_plain(self):
        """Test including namespaces without wildcards"""
        dest_mapping = DestMapping(namespace_set=["db1.col1", "db1.col2"])
        self.assertEqual(dest_mapping.unmap_namespace("db1.col1"), "db1.col1")
        self.assertEqual(dest_mapping.unmap_namespace("db1.col2"), "db1.col2")
        self.assertIsNone(dest_mapping.unmap_namespace("not.included"))
        self.assertEqual(dest_mapping.map_db("db1"), ["db1"])
        self.assertEqual(dest_mapping.map_db("not_included"), [])
        self.assertEqual(dest_mapping.map_namespace("db1.col1"), "db1.col1")
        self.assertEqual(dest_mapping.map_namespace("db1.col2"), "db1.col2")
        self.assertIsNone(dest_mapping.map_namespace("db1.col4"))

    def test_include_wildcard(self):
        """Test including namespaces with wildcards"""
        equivalent_dest_mappings = (
            DestMapping(namespace_set=["db1.*"]),
            DestMapping(user_mapping={"db1.*": {}}),
            DestMapping(user_mapping={"db1.*": {"rename": "db1.*"}}))
        for dest_mapping in equivalent_dest_mappings:
            self.assertEqual(dest_mapping.unmap_namespace("db1.col1"),
                             "db1.col1")
            self.assertEqual(dest_mapping.unmap_namespace("db1.col1"),
                             "db1.col1")
            self.assertEqual(dest_mapping.get("db1.col1"),
                             MappedNamespace("db1.col1"))
            self.assertListEqual(dest_mapping.map_db("db1"), ["db1"])
            self.assertEqual(dest_mapping.map_namespace("db1.col1"),
                             "db1.col1")
            self.assertIsNone(dest_mapping.map_namespace("db2.col4"))

    def test_include_wildcard_no_period_in_database(self):
        """Test that a database wildcard cannot match a period."""
        dest_mapping = DestMapping(namespace_set=["db*.col"])
        self.assertIsNone(dest_mapping.map_namespace("db.bar.col"))

    def test_exclude_plain(self):
        """Test excluding namespaces without wildcards"""
        dest_mapping = DestMapping(ex_namespace_set=["ex.clude"])
        self.assertEqual(dest_mapping.unmap_namespace("db.col"), "db.col")
        self.assertEqual(dest_mapping.unmap_namespace("ex.clude"), "ex.clude")
        self.assertEqual(dest_mapping.map_namespace("db.col"), "db.col")
        self.assertIsNone(dest_mapping.map_namespace("ex.clude"))

    def test_exclude_wildcard(self):
        """Test excluding namespaces with wildcards"""
        dest_mapping = DestMapping(ex_namespace_set=["ex.*"])
        self.assertEqual(dest_mapping.unmap_namespace("db.col"), "db.col")
        self.assertEqual(dest_mapping.unmap_namespace("ex.clude"), "ex.clude")
        self.assertEqual(dest_mapping.map_namespace("db.col"), "db.col")
        self.assertIsNone(dest_mapping.map_namespace("ex.clude"))
        self.assertIsNone(dest_mapping.map_namespace("ex.clude2"))

    def test_unmap_namespace_wildcard(self):
        """Test un-mapping a namespace that was never explicitly mapped."""
        dest_mapping = DestMapping(user_mapping={
            "db2.*": "db2.f*",
            "db_*.foo": "db_new_*.foo",
        })
        self.assertEqual(dest_mapping.unmap_namespace("db2.foo"), "db2.oo")
        self.assertEqual(dest_mapping.unmap_namespace("db_new_123.foo"),
                         "db_123.foo")

    def test_rename_validation(self):
        """Test namespace renaming validation."""
        # Multiple collections cannot be merged into the same target namespace
        with self.assertRaises(errors.InvalidConfiguration):
            DestMapping(user_mapping={
                "db1.col1": "newdb.newcol",
                "db2.col1": "newdb.newcol"})
        # Multiple collections cannot be merged into the same target namespace
        with self.assertRaises(errors.InvalidConfiguration):
            dest_mapping = DestMapping(user_mapping={
                "db*.col1": "newdb.newcol*",
                "db*.col2": "newdb.newcol*"})
            dest_mapping.map_namespace("db1.col1")
            dest_mapping.map_namespace("db1.col2")

    def test_fields_validation(self):
        """Test including/excluding fields per namespace."""
        # Cannot include and exclude fields in the same namespace
        with self.assertRaises(errors.InvalidConfiguration):
            DestMapping(user_mapping={
                "db.col": {"fields": ["a"], "excludeFields": ["b"]}})

        # Cannot include fields globally and then exclude fields
        with self.assertRaises(errors.InvalidConfiguration):
            DestMapping(include_fields=["a"], user_mapping={
                "db.col": {"excludeFields": ["b"]}})

        # Cannot exclude fields globally and then include fields
        with self.assertRaises(errors.InvalidConfiguration):
            DestMapping(exclude_fields=["b"], user_mapping={
                "db.col": {"fields": ["a"]}})

    def test_projection_include_wildcard(self):
        """Test include_fields on a wildcard namespace."""
        equivalent_dest_mappings = (
            DestMapping(include_fields=["foo", "nested.field"],
                        ex_namespace_set=["ignored.name"]),
            DestMapping(include_fields=["foo", "nested.field"],
                        namespace_set=["db.foo"]),
            DestMapping(include_fields=["foo", "nested.field"],
                        namespace_set=["db.*"]),
            DestMapping(user_mapping={
                "db.*": {"fields": ["foo", "nested.field"]}}),
            DestMapping(user_mapping={
                "db.foo": {"fields": ["foo", "nested.field"]}}),
            DestMapping(include_fields=["foo", "nested.field"], user_mapping={
                "db.*": {"fields": ["foo", "nested.field"]}})
        )
        for dest_mapping in equivalent_dest_mappings:
            self.assertEqual(dest_mapping.projection("db.foo"),
                             {"_id": 1, "foo": 1, "nested.field": 1})
            self.assertIsNone(dest_mapping.projection("ignored.name"))

    def test_projection_exclude_wildcard(self):
        """Test exclude_fields on a wildcard namespace."""
        equivalent_dest_mappings = (
            DestMapping(exclude_fields=["_id", "foo", "nested.field"],
                        namespace_set=["db.*"]),
            DestMapping(user_mapping={
                "db.*": {"excludeFields": ["_id", "foo", "nested.field"]}}),
            DestMapping(exclude_fields=["foo", "nested.field"], user_mapping={
                "db.*": {"excludeFields": ["_id", "foo", "nested.field"]}})
        )
        for dest_mapping in equivalent_dest_mappings:
            self.assertEqual(dest_mapping.projection("db.foo"),
                             {"foo": 0, "nested.field": 0})
            self.assertIsNone(dest_mapping.projection("ignored.name"))

    def test_match_replace_regex(self):
        """Test regex matching and replacing."""
        regex = re.compile(r"\Adb_([^.]*).foo\Z")
        self.assertIsNone(match_replace_regex(regex, "db.foo", "*.foo"))
        self.assertIsNone(match_replace_regex(regex, "db.foo.foo", "*.foo"))
        self.assertEqual(match_replace_regex(regex, "db_bar.foo", "*.foo"),
                         "bar.foo")

    def test_namespace_to_regex(self):
        """Test regex creation."""
        self.assertEqual(namespace_to_regex("db_*.foo"),
                         re.compile(r"\Adb_([^.]*).foo\Z"))
        self.assertEqual(namespace_to_regex("db.foo*"),
                         re.compile(r"\Adb.foo(.*)\Z"))
        self.assertEqual(namespace_to_regex("db.foo"),
                         re.compile(r"\Adb.foo\Z"))


class TestRegexSet(unittest.TestCase):
    """Test the RegexSet class."""

    def test_from_namespaces(self):
        """Test construction from list of namespaces."""
        self.assertEqual(RegexSet.from_namespaces([]),
                         RegexSet([], []))
        self.assertEqual(RegexSet.from_namespaces(["db.bar", "db_*.foo",
                                                   "db2.*"]),
                         RegexSet([namespace_to_regex("db_*.foo"),
                                   namespace_to_regex("db2.*")],
                                  ["db.bar"]))

    def test_contains(self):
        """Test membership query."""
        regex_set = RegexSet.from_namespaces(["db.bar", "db_*.foo", "db2.*"])
        self.assertTrue("db.bar" in regex_set)
        self.assertTrue("db_1.foo" in regex_set)
        self.assertTrue("db2.bar" in regex_set)
        self.assertTrue("db2.bar2" in regex_set)
        self.assertFalse("not.found" in regex_set)
        self.assertFalse("db_not.found" in regex_set)

    def test_add(self):
        """Test adding a new string."""
        regex_set = RegexSet.from_namespaces([])
        self.assertFalse("string.found" in regex_set)
        regex_set.add("string.found")
        self.assertTrue("string.found" in regex_set)

    def test_discard(self):
        """Test discarding a string."""
        regex_set = RegexSet.from_namespaces(["db.bar"])
        self.assertTrue("db.bar" in regex_set)
        regex_set.discard("db.bar")
        self.assertFalse("db.bar" in regex_set)

if __name__ == "__main__":
    unittest.main()
