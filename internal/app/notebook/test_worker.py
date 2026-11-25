import hashlib
import json
from pathlib import Path
import stat
import struct
import tempfile
import unittest
from unittest.mock import patch
import zipfile
import zlib

import worker


class MemoryDrive:
    def __init__(self, archives):
        self.archives = archives
        self.items = {"input": {"id": "input", "mimeType": worker.FOLDER}}
        self.bytes = {}
        self.attempted = []
        for name in archives:
            self.items[name] = {
                "id": name, "mimeType": "application/octet-stream",
                "parents": ["input"], "name": name, "size": str(len(archives[name])),
                "version": "1",
            }

    def get(self, file_id):
        return self.items[file_id]

    def children(self, parent, name=None):
        return [
            item for item in self.items.values()
            if parent in item.get("parents", []) and (name is None or item["name"] == name)
        ]

    def folder(self, parent, name):
        file_id = parent + "/" + name
        self.items.setdefault(file_id, {
            "id": file_id, "name": name, "parents": [parent], "mimeType": worker.FOLDER,
        })
        return file_id

    def download(self, item, target):
        self.attempted.append(item["id"])
        target.write_bytes(self.archives[item["id"]])

    def upload(self, parent, name, path, size, checksum):
        data = path.read_bytes()
        assert len(data) == size
        assert hashlib.md5(data, usedforsecurity=False).hexdigest() == checksum
        self.add(parent, name, data)

    def add(self, parent, name, data):
        file_id = parent + "/" + name
        self.items[file_id] = {
            "id": file_id, "name": name, "parents": [parent],
            "mimeType": "application/octet-stream",
        }
        self.bytes[file_id] = data

    move_archive = worker.Drive.move_archive

    def json(self, method, path, params, data):
        assert method == "PATCH"
        from urllib.parse import unquote
        item = self.items[unquote(path.removeprefix("files/"))]
        assert params["removeParents"] in item["parents"]
        item["parents"] = [params["addParents"]]
        item["name"] = data["name"]


def zip_archive(files):
    from io import BytesIO
    out = BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for name, data in files:
            z.writestr(name, data)
    return out.getvalue()


def rar_archive(name, content):
    # A tiny standalone RAR3 archive using the stored compression method.
    def header(kind, flags, body):
        serialized = struct.pack("<BHH", kind, flags, 7 + len(body)) + body
        return struct.pack("<H", zlib.crc32(serialized) & 0xffff) + serialized

    filename = name.encode("ascii")
    file_body = struct.pack(
        "<IIBIIBBHI",
        len(content), len(content), 3, zlib.crc32(content), 0x5C210000,
        20, 0x30, len(filename), stat.S_IFREG | 0o644,
    ) + filename
    return (
        b"Rar!\x1a\x07\x00"
        + header(0x73, 0, b"\x00" * 6)
        + header(0x74, 0x8000, file_body)
        + content
        + header(0x7B, 0, b"")
    )


class WorkerTests(unittest.TestCase):
    def run_job(self, drive, bad=None, retry=False):
        with tempfile.TemporaryDirectory() as scratch:
            directory = Path(scratch)
            (directory / "settings.json").write_text(json.dumps({
                "input_folder": "input", "retry_failed": retry, "bad_versions": bad or {},
            }), encoding="utf-8")
            with patch.object(worker, "Drive", return_value=drive):
                worker.run_worker(directory)
                worker.run_worker(directory)  # A completed report prevents a duplicate run.
            result = json.loads((directory / "report.json").read_text(encoding="utf-8"))
            self.assertTrue(result["done"])
            return result

    def test_zip_layout_collision_and_existing_output(self):
        drive = MemoryDrive({
            "movie.ZIP": zip_archive([
                ("nested/existing.txt", b"new"),
                ("nested/actual.txt", b"new output"),
                ("empty-dir/", b""),
                ("empty-file.txt", b""),
            ]),
            "readme.txt": b"untouched",
        })
        output = "input/uncompressed_files/movie"
        drive.folder("input", "uncompressed_files")
        drive.folder("input/uncompressed_files", "movie")
        drive.folder(output, "nested")
        drive.add(output + "/nested", "existing.txt", b"old")
        drive.folder("input", "compressed_files")
        drive.add("input/compressed_files", "movie.ZIP", b"existing archive")
        report = self.run_job(drive)
        self.assertEqual((report["scanned"], report["processed"], report["failures"]), (1, 1, []))
        self.assertEqual(drive.attempted, ["movie.ZIP"])
        self.assertEqual(drive.bytes[output + "/nested/existing.txt"], b"old")
        self.assertEqual(drive.bytes[output + "/nested/actual.txt"], b"new output")
        self.assertEqual(drive.bytes[output + "/empty-file.txt"], b"")
        self.assertEqual(drive.items[output + "/empty-dir"]["mimeType"], worker.FOLDER)
        self.assertEqual(drive.items["movie.ZIP"]["name"], "movie_1.ZIP")
        self.assertEqual(drive.items["movie.ZIP"]["parents"], ["input/compressed_files"])
        self.assertEqual(drive.items["readme.txt"]["parents"], ["input"])

    def test_rar(self):
        try:
            import rarfile  # noqa: F401
        except ImportError:
            self.skipTest("install rarfile==4.5 to test the real RAR reader")
        drive = MemoryDrive({"music.rar": rar_archive("folder/sample.txt", b"rar output")})
        report = self.run_job(drive)
        self.assertEqual((report["processed"], report["failures"]), (1, []))
        self.assertEqual(drive.bytes["input/uncompressed_files/music/folder/sample.txt"], b"rar output")

    def test_corruption_password_and_retry_by_version(self):
        encrypted = bytearray(zip_archive([("secret", b"data")]))
        local_flags = 6
        central_flags = encrypted.index(b"PK\x01\x02") + 8
        encrypted[local_flags] |= 1
        encrypted[central_flags] |= 1
        drive = MemoryDrive({"bad.zip": b"not a zip", "locked.zip": bytes(encrypted)})
        first = self.run_job(drive)
        self.assertEqual(
            [(f["name"], f["error"], f["permanent"]) for f in first["failures"]],
            [("bad.zip", "ZIP archive is corrupt", True), ("locked.zip", "Archive is password-protected", True)],
        )
        bad = {f["file_id"]: f["version"] for f in first["failures"]}
        again = self.run_job(drive, bad)
        self.assertEqual((again["scanned"], again["skipped"], again["failures"]), (2, 2, []))
        drive.items["bad.zip"]["version"] = "2"
        changed = self.run_job(drive, bad)
        self.assertEqual((changed["skipped"], len(changed["failures"])), (1, 1))
        forced = self.run_job(drive, bad, retry=True)
        self.assertEqual((forced["skipped"], len(forced["failures"])), (0, 2))
        self.assertEqual(drive.items["bad.zip"]["parents"], ["input"])

    def test_unsafe_path_and_link_are_rejected_before_publish(self):
        link = zipfile.ZipInfo("linked")
        link.create_system = 3
        link.external_attr = stat.S_IFLNK << 16
        drive = MemoryDrive({
            "traversal.zip": zip_archive([("safe.txt", b"data"), ("../outside", b"bad")]),
            "link.zip": zip_archive([(link, b"../outside")]),
            "good.zip": zip_archive([("ok", b"unrelated")]),
        })
        report = self.run_job(drive)
        self.assertEqual(report["processed"], 1)
        self.assertEqual(len(report["failures"]), 2)
        self.assertTrue(all(f["permanent"] for f in report["failures"]))
        self.assertNotIn("input/uncompressed_files/traversal/safe.txt", drive.bytes)
        self.assertIn("input/uncompressed_files/good/ok", drive.bytes)

    def test_insufficient_disk_space_keeps_archive_for_later(self):
        drive = MemoryDrive({"movie.zip": zip_archive([("large", b"some data")])})
        with patch.object(worker.shutil, "disk_usage", return_value=type("Disk", (), {"free": 0})()):
            report = self.run_job(drive)
        self.assertEqual(len(report["failures"]), 1)
        self.assertIn("disk space", report["failures"][0]["error"])
        self.assertFalse(report["failures"][0]["permanent"])
        self.assertEqual(drive.items["movie.zip"]["parents"], ["input"])

    def test_resumable_upload_probes_after_lost_response(self):
        from io import BytesIO
        with patch.object(worker, "CHUNK", 4), patch.object(worker.time, "sleep"):
            drive = worker.Drive(Path("unused"))
            interrupted = worker.DriveError("connection interrupted")
            responses = [interrupted, (308, {"Range": "bytes=0-3"}, b""), (201, {}, b"")]
            with patch.object(drive, "request", side_effect=responses) as http:
                drive._upload_session("https://upload", BytesIO(b"abcdefgh"), 8)
            self.assertEqual(
                [(call.kwargs["headers"]["Content-Range"], call.kwargs["data"]) for call in http.call_args_list],
                [("bytes 0-3/8", b"abcd"), ("bytes */8", b""), ("bytes 4-7/8", b"efgh")],
            )


if __name__ == "__main__":
    unittest.main()
