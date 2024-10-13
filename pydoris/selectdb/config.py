#! /usr/bin/python3

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import uuid


class WriteOptions:
    def __init__(self):
        self._headers = {
            'Content-Type': 'text/plain; charset=UTF-8',
            "Content-Length": None,
            "Transfer-Encoding": None,
            "Expect": "100-continue",
            'format': 'csv',
            "column_separator": ',',
        }

    def set_csv_format(self, column_separator):
        self._headers['format'] = 'csv'
        self._headers['column_separator'] = column_separator
        return self

    def set_json_format(self):
        self._headers['format'] = 'json'
        self._headers['read_json_by_line'] = 'true'
        return self

    def set_auto_uuid_label(self):
        self._headers['label'] = str(uuid.uuid4())
        return self

    def set_label(self, label):
        self._headers['label'] = label
        return self

    def set_format(self, data_format: str):
        if data_format.lower() in ['csv', 'json']:
            self._headers['format'] = data_format
        return self

    def set_line_delimiter(self, line_delimiter):
        self._headers['line_delimiter'] = line_delimiter
        return self

    def set_enable_profile(self):
        self._headers['enable_profile'] = 'true'
        return self

    def set_option(self, k, v):
        self._headers[k] = v
        return self

    def get_options(self):
        return self._headers