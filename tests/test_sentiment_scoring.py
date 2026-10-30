"""
Offline tests for sentiment scoring: prompt rendering, answer validation,
label-row shapes and provider request/response handling. No network, no BigQuery.
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).parent.parent))

from etl.sentiment_scoring import prompt as P
from etl.sentiment_scoring.config import load_config
from etl.sentiment_scoring.store import label_rows

COUNTRIES = [("KEN", "Kenya"), ("MAR", "Morocco"), ("DZA", "Algeria")]
CODES = {c for c, _ in COUNTRIES}
DOC = {
    "doc_id": "d1", "source_version": "v1", "source_id": "SRC_X",
    "title": "Titre", "body_text": "Corps", "language_iso639_3": "fra",
    "published_at": datetime(2026, 10, 1, 8, 0, tzinfo=timezone.utc),
}


class TestPrompt:
    def test_country_list_is_rendered_and_versions_follow_the_text(self):
        cfg = load_config()
        text = P.render_system_prompt(cfg.prompt_path, COUNTRIES)
        assert "KEN: Kenya" in text and "{country_list}" not in text
        v1 = P.model_version("m", "p", text)
        v2 = P.model_version("m", "p", text + " ")
        assert v1.startswith("m|p|") and v1 != v2

    def test_user_message_leaves_out_the_outlet(self):
        msg = P.user_message(DOC)
        assert "SRC_X" not in msg and "Titre" in msg and "Corps" in msg

    def test_schema_enumerates_countries_and_labels(self):
        item = P.response_schema(list(CODES))["properties"]["countries"]["items"]
        assert item["properties"]["country_iso3"]["enum"] == sorted(CODES)
        assert item["properties"]["sentiment_label"]["enum"] == list(P.LABELS)

    def test_free_country_schema_is_its_own_version(self):
        item = P.response_schema(list(CODES), free_country=True)["properties"]["countries"]["items"]
        assert item["properties"]["country_iso3"] == {"type": "string"}
        assert item["properties"]["sentiment_label"]["enum"] == list(P.LABELS)
        same, free = P.model_version("m", "p", "t"), P.model_version("m", "p", "t", "free_country")
        assert free.startswith("m|p+free_country|") and free.split("|")[2] == same.split("|")[2]


class TestValidate:
    def test_clean_answer(self):
        answer = json.dumps({"countries": [
            {"country_iso3": "MAR", "evidence": ["a", "b", "c"],
             "sentiment_label": "POSITIVE", "sentiment_score": 1.7},
            {"country_iso3": "DZA", "evidence": ["x" * 400],
             "sentiment_label": "negative", "sentiment_score": -0.6},
        ]})
        out = P.validate(answer, CODES)
        assert [c["country_iso3"] for c in out] == ["MAR", "DZA"]
        assert out[0]["sentiment_score"] == 1.0          # clipped
        assert len(out[0]["evidence"]) == 2              # capped
        assert len(out[1]["evidence"][0]) == 300         # truncated
        assert out[1]["sentiment_label"] == "NEGATIVE"

    def test_unknown_and_duplicate_countries_are_dropped(self):
        answer = json.dumps({"countries": [
            {"country_iso3": "FRA", "evidence": [], "sentiment_label": "NEUTRAL", "sentiment_score": 0},
            {"country_iso3": "KEN", "evidence": [], "sentiment_label": "NEUTRAL", "sentiment_score": 0},
            {"country_iso3": "KEN", "evidence": [], "sentiment_label": "POSITIVE", "sentiment_score": 1},
        ]})
        out = P.validate(answer, CODES)
        assert [(c["country_iso3"], c["sentiment_label"]) for c in out] == [("KEN", "NEUTRAL")]

    def test_fenced_json_is_accepted_and_empty_answers_mean_no_countries(self):
        assert P.validate('```json\n{"countries": []}\n```', CODES) == []
        assert P.validate("{}", CODES) == []

    @pytest.mark.parametrize("bad", ["not json", '{"other": 1}', '{"countries": [{"country_iso3": "KEN", '
                                     '"evidence": [], "sentiment_label": "HAPPY", "sentiment_score": 0}]}'])
    def test_unusable_answers_raise(self, bad):
        with pytest.raises(P.ResponseError):
            P.validate(bad, CODES)


class TestLabelRows:
    def test_tagged_rows(self):
        rows = label_rows(DOC, "mv", "run1", countries=[
            {"country_iso3": "KEN", "evidence": ["q"], "sentiment_label": "NEGATIVE", "sentiment_score": -0.5}])
        assert len(rows) == 1
        r = rows[0]
        assert (r["tag_status"], r["country_iso3"], r["labeler_type"], r["labeler_id"]) == \
            ("TAGGED", "KEN", "MODEL", "mv")
        assert str(r["published_date"]) == "2026-10-01"

    def test_no_country_and_error_rows(self):
        assert label_rows(DOC, "mv", "r", countries=[])[0]["tag_status"] == "NO_COUNTRY"
        err = label_rows(DOC, "mv", "r", error="bad json")[0]
        assert err["tag_status"] == "ERROR" and err["country_iso3"] is None
        assert err["error_detail"] == "bad json"


class TestConfig:
    def test_three_models_with_unique_slots(self):
        cfg = load_config()
        assert {m.slot for m in cfg.models} == {"gemini", "claude", "gemma"}
        assert {m.call_mode for m in cfg.models} <= {"BATCH", "SERIAL"}

    def test_only_gemma_uses_the_free_country_field(self):
        cfg = load_config()
        assert {m.slot: m.country_field for m in cfg.models} == \
            {"gemini": "enum", "claude": "enum", "gemma": "free"}
