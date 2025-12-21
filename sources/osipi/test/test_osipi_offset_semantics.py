from __future__ import annotations

from typing import Any


def test_snapshot_tables_do_not_repeat_after_done_offset(monkeypatch):
    """
    Snapshot tables should be idempotent under streaming semantics:
    once they return {"offset": "done"}, subsequent reads with start_offset={"offset":"done"}
    must return no rows to avoid duplicates.
    """

    from sources.osipi.osipi import LakeflowConnect

    conn = LakeflowConnect(
        {
            "pi_base_url": "https://example.invalid",
            "access_token": "dummy",
        }
    )

    def fake_get_json(_self: LakeflowConnect, path: str, params: Any = None) -> dict:
        assert path == "/piwebapi/dataservers"
        return {"Items": [{"WebId": "DS1", "Name": "PI"}]}

    monkeypatch.setattr(LakeflowConnect, "_get_json", fake_get_json, raising=True)

    it1, off1 = conn.read_table("pi_dataservers", {}, {})
    rows1 = list(it1)
    assert rows1 == [{"webid": "DS1", "name": "PI"}]
    assert off1 == {"offset": "done"}

    it2, off2 = conn.read_table("pi_dataservers", off1, {})
    rows2 = list(it2)
    assert rows2 == []
    assert off2 == {"offset": "done"}


