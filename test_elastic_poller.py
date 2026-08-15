"""Unit tests for elastic_poller bookmark, query, pagination, and event mapping."""

import json
import os
import tempfile
import unittest
from unittest.mock import patch

import common_event
import elastic_poller


SAMPLE_HIT = {
    "_index": ".ds-.kibana-event-log-ds-2026.08.08-000027",
    "_id": "43365083-0423-40ea-b045-7362858aad31",
    "_score": None,
    "_source": {
        "@timestamp": "2026-08-13T17:14:18.365Z",
        "event": {
            "provider": "alerting",
            "action": "execute-start",
            "kind": "alert",
            "category": ["logs"],
            "start": "2026-08-13T17:14:18.365Z",
        },
        "kibana": {
            "alert": {
                "rule": {
                    "rule_type_id": "logs.alert.document.count",
                    "consumer": "alerts",
                    "execution": {
                        "uuid": "f025a6bf-38fb-45e3-b477-9555fc64b642",
                    },
                },
            },
            "saved_objects": [
                {
                    "rel": "primary",
                    "type": "alert",
                    "id": "c7707340-424b-11ee-ae1d-b3a96bbcb023",
                    "type_id": "logs.alert.document.count",
                    "namespace": "cnrwm",
                }
            ],
            "space_ids": ["cnrwm"],
        },
        "rule": {
            "id": "c7707340-424b-11ee-ae1d-b3a96bbcb023",
            "license": "basic",
            "category": "logs.alert.document.count",
            "ruleset": "logs",
        },
        "message": 'rule execution start: "c7707340-424b-11ee-ae1d-b3a96bbcb023"',
    },
    "sort": [1786641258365, "43365083-0423-40ea-b045-7362858aad31"],
}


class BuildLogsQueryTests(unittest.TestCase):
    def test_uses_exclusive_gt_with_epoch_millis(self):
        bookmark_ms = 1786641258556
        query = elastic_poller.build_logs_query(text="*", bookmark_ms=bookmark_ms, size=500)

        range_filter = query["query"]["bool"]["filter"][0]["range"]["@timestamp"]
        self.assertEqual(range_filter["gt"], bookmark_ms)
        self.assertEqual(range_filter["format"], "epoch_millis")
        self.assertNotIn("gte", range_filter)

    def test_sort_includes_timestamp_and_id_tiebreaker(self):
        query = elastic_poller.build_logs_query(text="*", bookmark_ms=0, size=500)
        self.assertEqual(
            query["sort"],
            [{"@timestamp": {"order": "asc"}}, {"_id": {"order": "asc"}}],
        )

    def test_search_after_included_when_provided(self):
        search_after = [1786641258365, "doc-id"]
        query = elastic_poller.build_logs_query(
            text="*",
            bookmark_ms=1786641258000,
            size=500,
            search_after=search_after,
        )
        self.assertEqual(query["search_after"], search_after)

    def test_bookmark_ms_not_truncated_to_seconds(self):
        """Bookmark 8556ms must not collapse to same query as 8000ms."""
        query_a = elastic_poller.build_logs_query(text="*", bookmark_ms=1786641258556, size=500)
        query_b = elastic_poller.build_logs_query(text="*", bookmark_ms=1786641258000, size=500)
        self.assertNotEqual(
            query_a["query"]["bool"]["filter"][0]["range"]["@timestamp"]["gt"],
            query_b["query"]["bool"]["filter"][0]["range"]["@timestamp"]["gt"],
        )


class HitTimestampTests(unittest.TestCase):
    def test_hit_timestamp_ms_from_iso_string(self):
        self.assertEqual(
            elastic_poller.hit_timestamp_ms(SAMPLE_HIT),
            1786641258365,
        )


class BookmarkFileTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_bookmark_dir = elastic_poller.bookmark_dir
        self.original_bookmark_file = elastic_poller.bookmark_file
        elastic_poller.bookmark_dir = self.temp_dir.name
        elastic_poller.bookmark_file = os.path.join(
            self.temp_dir.name, "testorg.elastic.bookmark"
        )

    def tearDown(self):
        elastic_poller.bookmark_dir = self.original_bookmark_dir
        elastic_poller.bookmark_file = self.original_bookmark_file
        self.temp_dir.cleanup()

    def test_set_and_get_bookmark_roundtrip(self):
        elastic_poller.setBookmark(1786641258556)
        self.assertEqual(elastic_poller.getBookmark(), 1786641258556)

    def test_get_bookmark_creates_file_with_zero(self):
        self.assertEqual(elastic_poller.getBookmark(), 0)


class PollCycleTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_bookmark_dir = elastic_poller.bookmark_dir
        self.original_bookmark_file = elastic_poller.bookmark_file
        elastic_poller.bookmark_dir = self.temp_dir.name
        elastic_poller.bookmark_file = os.path.join(
            self.temp_dir.name, "testorg.elastic.bookmark"
        )
        elastic_poller.ELASTIC_BATCH_SIZE = 500

    def tearDown(self):
        elastic_poller.bookmark_dir = self.original_bookmark_dir
        elastic_poller.bookmark_file = self.original_bookmark_file
        self.temp_dir.cleanup()

    @patch.object(elastic_poller, "send_event", return_value=True)
    @patch.object(elastic_poller, "fetch_elasticsearch_hits")
    def test_poll_cycle_advances_bookmark_on_success(self, mock_fetch, mock_send):
        mock_fetch.return_value = ([SAMPLE_HIT], 5)
        result = elastic_poller.poll_cycle(1786641258000, 1786641258000, True)
        self.assertEqual(result, 1786641258365)
        self.assertEqual(elastic_poller.getBookmark(), 1786641258365)
        mock_send.assert_called_once()

    @patch.object(elastic_poller, "send_event", return_value=False)
    @patch.object(elastic_poller, "fetch_elasticsearch_hits")
    def test_poll_cycle_does_not_advance_bookmark_on_delivery_failure(self, mock_fetch, mock_send):
        elastic_poller.setBookmark(1786641258000)
        mock_fetch.return_value = ([SAMPLE_HIT], 5)
        result = elastic_poller.poll_cycle(1786641258000, 1786641258000, True)
        self.assertEqual(result, 1786641258000)
        self.assertEqual(elastic_poller.getBookmark(), 1786641258000)

    @patch.object(elastic_poller, "send_event", return_value=True)
    @patch.object(elastic_poller, "fetch_elasticsearch_hits")
    def test_poll_cycle_paginates_with_search_after(self, mock_fetch, mock_send):
        page_size = 2
        elastic_poller.ELASTIC_BATCH_SIZE = page_size
        hit_a = dict(SAMPLE_HIT)
        hit_b = dict(SAMPLE_HIT, _id="bbbb", sort=[1786641258365, "bbbb"])
        hit_c = dict(SAMPLE_HIT, _id="cccc", sort=[1786641259000, "cccc"])
        hit_c["_source"] = dict(SAMPLE_HIT["_source"])
        hit_c["_source"]["@timestamp"] = "2026-08-13T17:14:19.000Z"

        mock_fetch.side_effect = [
            ([hit_a, hit_b], 5),
            ([hit_c], 3),
        ]

        result = elastic_poller.poll_cycle(1786641258000, 1786641258000, True)
        self.assertEqual(mock_fetch.call_count, 2)
        self.assertEqual(mock_fetch.call_args_list[1].kwargs["search_after"], [1786641258365, "bbbb"])
        self.assertEqual(result, 1786641259000)


class EventMappingTests(unittest.TestCase):
    def test_event_time_matches_source_timestamp(self):
        event = common_event.CommonEvent.new_from_file(
            mapping_file_name="elastic_event_mappings.yaml",
            mapping_file_path=".",
            original_record=SAMPLE_HIT,
        )
        cef = event.get_cef()["cef"]
        self.assertTrue(cef["event_time"].startswith("2026-08-13T17:14:18.365"))
        self.assertTrue(cef["event_time"].endswith("Z"))

    def test_event_id_uses_execution_uuid(self):
        event = common_event.CommonEvent.new_from_file(
            mapping_file_name="elastic_event_mappings.yaml",
            mapping_file_path=".",
            original_record=SAMPLE_HIT,
        )
        cef = event.get_cef()["cef"]
        self.assertEqual(cef["event_id"], "f025a6bf-38fb-45e3-b477-9555fc64b642")

    def test_event_id_falls_back_to_document_id(self):
        hit = json.loads(json.dumps(SAMPLE_HIT))
        del hit["_source"]["kibana"]["alert"]["rule"]["execution"]
        event = common_event.CommonEvent.new_from_file(
            mapping_file_name="elastic_event_mappings.yaml",
            mapping_file_path=".",
            original_record=hit,
        )
        cef = event.get_cef()["cef"]
        self.assertEqual(cef["event_id"], SAMPLE_HIT["_id"])

    def test_event_id_is_stable_across_mapping_calls(self):
        ids = []
        for _ in range(3):
            event = common_event.CommonEvent.new_from_file(
                mapping_file_name="elastic_event_mappings.yaml",
                mapping_file_path=".",
                original_record=SAMPLE_HIT,
            )
            ids.append(event.get_cef()["cef"]["event_id"])
        self.assertEqual(len(set(ids)), 1)


if __name__ == "__main__":
    unittest.main()
