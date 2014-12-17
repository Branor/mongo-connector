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

"""A class to serve as proxy for the target engine for testing.

Receives documents from the oplog worker threads and indexes them
into the backend.

Please look at the Solr and ElasticSearch doc manager classes for a sample
implementation with real systems.
"""

import itertools
from urlparse import urlparse

from mongo_connector.errors import OperationFailed
from mongo_connector.doc_managers import DocManagerBase
from mongo_connector.compat import u
from bson import json_util, ObjectId

from couchbase import Couchbase, FMT_JSON
from couchbase.exceptions import CouchbaseError
from couchbase.views.iterator import View
from couchbase.views.params import Query

class DocManager(DocManagerBase):
    """Couchbase implementation of the DocManager interface.

    Receives documents from an OplogThread and replicates them to Couchbase.
    """

    def __init__(self, url='localhost', port=8091, bucket='default', unique_key='_id', **kwargs):
        """Creates a connection to the specified Couchbase cluster and bucket.
        """
        print kwargs
        parsed_url = urlparse(url)
        if parsed_url.hostname != None and parsed_url.port != None: 
            url = parsed_url.hostname
            port = parsed_url.port
        if parsed_url.path != None:
            path = parsed_url.path.strip("/").strip(" ")
            if path != None and path != "":
                bucket = path

        self.couchbase = Couchbase.connect(bucket=bucket, host=url, port=port, quiet=True)
        self.unique_key = unique_key
        self.url = url
        self.bucket = bucket

    def stop(self):
        """Stops any running threads in the DocManager.
        """
        pass

    def apply_update(self, doc, update_spec):
        if "$set" not in update_spec and "$unset" not in update_spec:
            # Don't try to add ns and _ts fields back in from doc
            return update_spec
        return super(DocManager, self).apply_update(doc, update_spec)

    def update(self, doc, update_spec):
        print "--> update"
        """Apply updates given in update_spec to the document whose id
        matches that of doc.

        """
        retries = 100

        doc["_id"] = str(doc["_id"])
        doc_id = doc["_id"]

        while retries > 0:
            retries = retries - 1
            result = self.couchbase.get(doc_id)
            updated = self.apply_update(result.value, update_spec)

            updated["_id"] = str(updated["_id"])

            try:
                self.couchbase.replace(doc_id, updated, cas=result.cas)
                break

            except KeyExistsError:
                print "CAS error, retrying"        
        
        return updated

    def upsert(self, doc):
        print "--> upsert"
        """Adds a document to the doc dict.
        """

        # Allow exceptions to be triggered (for testing purposes)
        if doc.get('_upsert_exception'):
            raise Exception("upsert exception")

        doc["_id"] = str(doc["_id"])
        self.couchbase.set(doc["_id"], doc)

    def remove(self, doc):
        print "--> remove"
        """Removes the document from the doc dict.
        """
        doc_id = doc["_id"]
        self.couchbase.delete(doc_id)

    def search(self, start_ts, end_ts):
        print '--> search: ', start_it, end_ts
        """Searches through all documents and finds all documents that were
        modified or deleted within the range.
        """
        q = Query(inclusive_end=False)
        q.mapkey_range = [start_ts, end_ts + q.STRING_RANGE_END]
        view = View(self.couchbase, "mongo_connect", "by_timestamp", query=q, include_docs=True)

        for row in view:
            print row
            yield result.doc.value

    def commit(self):
        print "--> commit"
        """Simply passes since we're not using an engine that needs commiting.
        """
        pass

    def get_last_doc(self):
        print "--> get_last_doc"
        """Searches through the doc dict to find the document that was
        modified or deleted most recently."""

        q = Query(descending=True, limit=1)
        view = View(self.couchbase, "dev_mongo_connect", "by_timestamp", query=q, include_docs=True)

        for row in view:
            print row
            return result.doc.value
