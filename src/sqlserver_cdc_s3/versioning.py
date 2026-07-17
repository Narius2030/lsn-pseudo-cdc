"""SQL Server version classification helpers."""

from __future__ import annotations


def parse_version_tuple(value: str) -> tuple[int, ...]:
    return tuple(int(part) for part in value.split("."))


def classify_sqlserver_2014_sp1_build(build_tuple: tuple[int, ...]) -> dict[str, str] | None:
    sp1_builds: list[dict[str, object]] = [
        {"builds": [(12, 0, 4100, 1)], "cu_label": "SP1 RTM", "kb": "KB3058865", "release_date": "2015-05-04"},
        {"builds": [(12, 0, 4213, 0)], "cu_label": "SP1 GDR (MS15-058)", "kb": "KB3070446", "release_date": "2015-07-14"},
        {"builds": [(12, 0, 4232, 0)], "cu_label": "SP1 GDR", "kb": "KB3194720", "release_date": "2016-11-08"},
        {"builds": [(12, 0, 4237, 0)], "cu_label": "SP1 GDR", "kb": "KB4019091", "release_date": "2017-08-08"},
        {"builds": [(12, 0, 4416, 1)], "cu_label": "SP1 CU1", "kb": "KB3067839", "release_date": "2015-06-19"},
        {"builds": [(12, 0, 4422, 0)], "cu_label": "SP1 CU2", "kb": "KB3075950", "release_date": "2015-08-17"},
        {"builds": [(12, 0, 4427, 24)], "cu_label": "SP1 CU3", "kb": "KB3094221", "release_date": "2015-10-19"},
        {"builds": [(12, 0, 4436, 0)], "cu_label": "SP1 CU4", "kb": "KB3106660", "release_date": "2015-12-21"},
        {"builds": [(12, 0, 4439, 1)], "cu_label": "SP1 CU5", "kb": "KB3130926", "release_date": "2016-02-22"},
        {"builds": [(12, 0, 4449, 0)], "cu_label": "SP1 CU6 (Deprecated)", "kb": "KB3144524", "release_date": "2016-04-18"},
        {"builds": [(12, 0, 4457, 0)], "cu_label": "SP1 CU6", "kb": "KB3167392", "release_date": "2016-05-30"},
        {"builds": [(12, 0, 4459, 0)], "cu_label": "SP1 CU7", "kb": "KB3162659", "release_date": "2016-06-20"},
        {"builds": [(12, 0, 4468, 0)], "cu_label": "SP1 CU8", "kb": "KB3174038", "release_date": "2016-08-15"},
        {"builds": [(12, 0, 4474, 0)], "cu_label": "SP1 CU9", "kb": "KB3186964", "release_date": "2016-10-17"},
        {"builds": [(12, 0, 4491, 0)], "cu_label": "SP1 CU10", "kb": "KB3204399", "release_date": "2016-12-19"},
        {"builds": [(12, 0, 4502, 0)], "cu_label": "SP1 CU11", "kb": "KB4010392", "release_date": "2017-02-21"},
        {"builds": [(12, 0, 4511, 0)], "cu_label": "SP1 CU12", "kb": "KB4017793", "release_date": "2017-04-17"},
        {
            "builds": [(12, 0, 4520, 0), (12, 0, 4522, 0)],
            "cu_label": "SP1 CU13",
            "kb": "KB4019099",
            "release_date": "2017-08-08",
            "notes": (
                "Microsoft build tables list SP1 CU13 as 12.0.4520.0, while the KB4019099 download package "
                "is published with version 12.0.4522.0. Treat both as SP1 CU13."
            ),
        },
    ]

    for item in sp1_builds:
        if build_tuple in item["builds"]:
            result = {
                "cu_label": str(item["cu_label"]),
                "kb": str(item["kb"]),
                "release_date": str(item["release_date"]),
            }
            notes = item.get("notes")
            if notes:
                result["notes"] = str(notes)
            return result
    return None
