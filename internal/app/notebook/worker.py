"""Drive archive worker installed as a managed Colab notebook cell."""

import hashlib
import http.client
from importlib.metadata import PackageNotFoundError, version
import json
import lzma
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from urllib import error, parse, request
import uuid
import zipfile
import zlib

DRIVE = "https://www.googleapis.com/drive/v3/"
UPLOAD = "https://www.googleapis.com/upload/drive/v3/files"
FOLDER = "application/vnd.google-apps.folder"
FILE_FIELDS = "id,name,mimeType,size,md5Checksum,version,parents,trashed"
CHUNK = 8 * 1024 * 1024
DISK_RESERVE = 64 * 1024 * 1024


class DriveError(Exception):
    def __init__(self, operation, status=0, retryable=False):
        self.status = status
        self.retryable = retryable or status in (0, 401, 408, 429) or status >= 500
        suffix = f" (HTTP {status})" if status else ""
        super().__init__(operation + suffix)


class ArchiveError(Exception):
    def __init__(self, message, permanent=False):
        self.permanent = permanent
        super().__init__(message)


class NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def safe_detail(exc):
    if isinstance(exc, (ArchiveError, DriveError)):
        return str(exc)
    if isinstance(exc, OSError):
        return "Colab file I/O failed" + (f" (errno {exc.errno})" if exc.errno else "")
    return f"Colab worker error ({type(exc).__name__})"


def drive_literal(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


class Drive:
    def __init__(self, token_file):
        self.token_file = token_file
        self.opener = request.build_opener(NoRedirect())

    def _request(self, method, url, headers=None, data=None, limit=2 * 1024 * 1024):
        parsed = parse.urlsplit(url)
        if (
            parsed.scheme != "https"
            or parsed.netloc != "www.googleapis.com"
            or not parsed.path.startswith(("/drive/v3/", "/upload/drive/v3/"))
        ):
            raise DriveError("Drive returned an unexpected request URL", 400)
        try:
            access_token = json.loads(self.token_file.read_text(encoding="utf-8"))["access_token"]
        except (OSError, ValueError, KeyError):
            raise DriveError("Colab could not read its current Drive access token") from None
        if not isinstance(access_token, str) or not access_token:
            raise DriveError("Colab has no current Drive access token")
        req = request.Request(
            url, data=data, method=method,
            headers={**(headers or {}), "Authorization": "Bearer " + access_token},
        )
        try:
            with self.opener.open(req, timeout=45) as response:
                body = response.read(limit + 1)
                if len(body) > limit:
                    raise DriveError("Drive response exceeded the expected size", 502)
                return response.status, response.headers, body
        except error.HTTPError as response:
            with response:
                return response.code, response.headers, response.read(65536)
        except (OSError, http.client.HTTPException):
            raise DriveError("Drive connection interrupted") from None

    def request(self, method, url, *, expect=(200,), attempts=4, **kwargs):
        for attempt in range(attempts):
            try:
                status, headers, body = self._request(method, url, **kwargs)
                if status in expect:
                    return status, headers, body
                rate_limited = status == 403 and any(
                    reason in body for reason in (b"rateLimitExceeded", b"userRateLimitExceeded")
                )
                raise DriveError("Drive request failed", status, rate_limited)
            except DriveError as exc:
                if not exc.retryable or attempt == attempts - 1:
                    raise
                time.sleep(2 ** attempt)

    def json(self, method, path, params=None, data=None, expect=(200,)):
        url = DRIVE + path
        if params:
            url += "?" + parse.urlencode(params)
        headers = {"Content-Type": "application/json"} if data is not None else None
        result = self.request(
            method, url, expect=expect, headers=headers,
            data=json.dumps(data).encode() if data is not None else None,
        )
        try:
            return json.loads(result[2])
        except ValueError:
            raise DriveError("Drive returned invalid metadata", 502) from None

    def get(self, file_id):
        return self.json("GET", "files/" + parse.quote(file_id, safe=""), {"fields": FILE_FIELDS})

    def children(self, parent, name=None):
        query = f"trashed = false and {drive_literal(parent)} in parents"
        if name is not None:
            query += f" and name = {drive_literal(name)}"
        params = {"q": query, "fields": f"nextPageToken,files({FILE_FIELDS})", "pageSize": 100}
        while True:
            page = self.json("GET", "files", params)
            yield from page.get("files", [])
            if not page.get("nextPageToken"):
                return
            params["pageToken"] = page["nextPageToken"]

    def new_id(self):
        return self.json("GET", "files/generateIds", {"count": 1, "space": "drive"})["ids"][0]

    def folder(self, parent, name):
        matches = list(self.children(parent, name))
        if len(matches) == 1 and matches[0]["mimeType"] == FOLDER:
            return matches[0]["id"]
        if matches:
            raise ArchiveError("An output folder name is ambiguous or already used by a file")
        file_id = self.new_id()
        metadata = {"id": file_id, "name": name, "parents": [parent], "mimeType": FOLDER}
        try:
            self.json("POST", "files", data=metadata, expect=(200, 201))
        except DriveError as exc:
            if exc.status != 409:
                raise
            existing = self.get(file_id)
            if existing["mimeType"] != FOLDER or existing["name"] != name or parent not in existing["parents"]:
                raise
        return file_id

    def download(self, item, target, on_bytes):
        size = int(item["size"])
        require_space(target.parent, size)
        url = DRIVE + "files/" + parse.quote(item["id"], safe="") + "?alt=media"
        digest = hashlib.md5(usedforsecurity=False)
        with target.open("wb") as destination:
            for start in range(0, size, CHUNK):
                end = min(start + CHUNK, size) - 1
                amount = end - start + 1
                status, headers, body = self.request(
                    "GET", url, expect=(200, 206), limit=amount,
                    headers={"Range": f"bytes={start}-{end}", "Accept-Encoding": "identity"},
                )
                if len(body) != amount or (
                    status == 206 and headers.get("Content-Range") != f"bytes {start}-{end}/{size}"
                ) or (status == 200 and amount != size):
                    raise DriveError("Drive returned an incomplete archive range", 502)
                destination.write(body)
                digest.update(body)
                on_bytes(end + 1)
        if item.get("md5Checksum") and digest.hexdigest() != item["md5Checksum"]:
            raise ArchiveError("Archive content changed or was incomplete during download")

    def _upload_empty(self, file_id, metadata):
        boundary = "colab-" + uuid.uuid4().hex
        payload = (
            f"--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n"
            + json.dumps(metadata)
            + f"\r\n--{boundary}\r\nContent-Type: application/octet-stream\r\n\r\n\r\n--{boundary}--\r\n"
        ).encode()
        try:
            self.request("POST", UPLOAD + "?uploadType=multipart", expect=(200, 201),
                         headers={"Content-Type": "multipart/related; boundary=" + boundary},
                         data=payload)
        except DriveError as exc:
            if exc.status != 409:
                raise
            existing = self.get(file_id)
            if existing.get("size") != "0":
                raise

    def _upload_session(self, location, source, size, on_bytes):
        offset = 0
        stalled = 0
        while offset < size:
            source.seek(offset)
            data = source.read(min(CHUNK, size - offset))
            if not data:
                raise ArchiveError("Staged file ended before its expected size")
            try:
                status, headers, _ = self.request(
                    "PUT", location, expect=(200, 201, 308), attempts=1, data=data,
                    headers={"Content-Range": f"bytes {offset}-{offset + len(data) - 1}/{size}"},
                )
            except DriveError as exc:
                if not exc.retryable:
                    raise
                time.sleep(min(2 ** stalled, 8))
                status, headers, _ = self.request(
                    "PUT", location, expect=(200, 201, 308), data=b"",
                    headers={"Content-Range": f"bytes */{size}"},
                )
            if status in (200, 201):
                on_bytes(size)
                return
            received = headers.get("Range", "")
            match = re.fullmatch(r"bytes=0-(\d+)", received)
            if received and not match:
                raise DriveError("Drive returned an invalid upload range", 502)
            new_offset = int(match[1]) + 1 if match else 0
            if new_offset < offset or new_offset > offset + len(data) or new_offset >= size:
                raise DriveError("Drive returned an unexpected upload offset", 502)
            stalled = stalled + 1 if new_offset == offset else 0
            if stalled >= 4:
                raise DriveError("Drive resumable upload stopped making progress", 502)
            offset = new_offset
            on_bytes(offset)

    def upload(self, parent, name, path, size, checksum, on_bytes):
        file_id = self.new_id()
        metadata = {"id": file_id, "name": name, "parents": [parent]}
        if not size:
            self._upload_empty(file_id, metadata)
            return
        for attempt in range(3):
            on_bytes(0)
            try:
                _, headers, _ = self.request(
                    "POST", UPLOAD + "?uploadType=resumable", expect=(200, 201),
                    headers={
                        "Content-Type": "application/json; charset=UTF-8",
                        "X-Upload-Content-Length": str(size),
                        "X-Upload-Content-Type": "application/octet-stream",
                    },
                    data=json.dumps(metadata).encode(),
                )
                location = headers.get("Location")
                if not location:
                    raise DriveError("Drive did not provide a resumable upload URL", 502)
                with path.open("rb") as source:
                    self._upload_session(location, source, size, on_bytes)
                return
            except DriveError as exc:
                if exc.status not in (404, 409, 410) and not exc.retryable:
                    raise
                try:
                    written = self.get(file_id)
                except DriveError as lookup:
                    if lookup.status != 404:
                        raise
                    if attempt == 2:
                        raise exc from None
                    continue
                if written.get("size") == str(size) and written.get("md5Checksum") == checksum:
                    on_bytes(size)
                    return
                raise DriveError("Drive upload metadata did not match the staged file", 502)

    def move_archive(self, item, source, destination):
        file_id = item["id"]
        current = self.get(file_id)
        if current.get("trashed") or current.get("version") != item.get("version") or source not in current.get("parents", []):
            raise ArchiveError("Archive changed in Drive before it could be moved")
        names = {child["name"] for child in self.children(destination)}
        name = item["name"]
        stem, extension = os.path.splitext(name)
        suffix = 1
        while name in names:
            name = f"{stem}_{suffix}{extension}"
            suffix += 1
        try:
            self.json(
                "PATCH", "files/" + parse.quote(file_id, safe=""),
                {"addParents": destination, "removeParents": source, "fields": FILE_FIELDS},
                {"name": name},
            )
        except DriveError:
            current = self.get(file_id)
            if destination not in current.get("parents", []) or current.get("name") != name:
                raise


def require_space(directory, size):
    if size + DISK_RESERVE > shutil.disk_usage(directory).free:
        raise ArchiveError("Colab runtime has insufficient free disk space for this archive or member")


def safe_parts(name, is_dir):
    name = name[:-1] if is_dir and name.endswith("/") else name
    if "\\" in name or re.match(r"^[a-zA-Z]:", name):
        raise ArchiveError("Archive contains an unsafe path", True)
    parts = name.split("/")
    if any(
        piece in ("", ".", "..")
        or re.match(r"^[a-zA-Z]:", piece)
        or any(ord(ch) < 32 or ord(ch) == 127 for ch in piece)
        for piece in parts
    ):
        raise ArchiveError("Archive contains an unsafe path", True)
    return tuple(parts)


def archive_members(archive, is_zip):
    if not is_zip:
        import rarfile
    files = set()
    directories = set()
    result = []
    for entry in archive.infolist():
        directory = entry.is_dir()
        if is_zip:
            mode = entry.external_attr >> 16 if entry.create_system == 3 else 0
            unsafe = False
            if entry.flag_bits & 1:
                raise ArchiveError("Archive is password-protected", True)
            name = entry.orig_filename
        else:
            mode = entry.mode if entry.host_os == rarfile.RAR_OS_UNIX else 0
            unsafe = (
                entry.is_symlink() or entry.file_redir is not None
                or not (directory or entry.is_file())
            )
            if entry.needs_password():
                raise ArchiveError("Archive is password-protected", True)
            name = entry.filename
        if unsafe or stat.S_IFMT(mode) not in (0, stat.S_IFDIR if directory else stat.S_IFREG):
            raise ArchiveError("Archive contains a link or special file", True)
        parts = safe_parts(name, directory)
        for index in range(1, len(parts)):
            directories.add(parts[:index])
        if directory:
            directories.add(parts)
        else:
            if parts in files:
                continue  # Match the original worker's first-file-wins behavior.
            files.add(parts)
        result.append((entry, parts, directory))
    if files & directories:
        raise ArchiveError("Archive uses the same path for a file and a directory", True)
    return result


def extract_members(archive, is_zip, drive, output_parent, name, scratch, progress):
    members = archive_members(archive, is_zip)
    root = drive.folder(output_parent, name)
    folders = {(): root}
    staged = scratch / "member"
    for entry, parts, is_dir in members:
        for index in range(1, len(parts) + int(is_dir)):
            path = parts[:index]
            if path not in folders:
                folders[path] = drive.folder(folders[path[:-1]], path[-1])
        if is_dir:
            continue
        parent = folders[parts[:-1]]
        existing = list(drive.children(parent, parts[-1]))
        if any(child["mimeType"] != FOLDER for child in existing):
            continue
        if existing:
            raise ArchiveError("An extracted file path is already used by a Drive folder")
        require_space(scratch, entry.file_size)
        checksum = hashlib.md5(usedforsecurity=False)
        member = "/".join(parts)
        progress.phase("extracting", entry.file_size, member)
        try:
            with archive.open(entry) as source, staged.open("wb") as destination:
                remaining = entry.file_size
                while remaining:
                    data = source.read(min(CHUNK, remaining))
                    if not data:
                        raise ArchiveError("Archive member is truncated", True)
                    destination.write(data)
                    checksum.update(data)
                    remaining -= len(data)
                    progress.bytes_done(entry.file_size - remaining)
                if source.read(1):
                    raise ArchiveError("Archive member exceeds its declared size", True)
            progress.phase("uploading", entry.file_size, member)
            drive.upload(
                parent, parts[-1], staged, entry.file_size, checksum.hexdigest(), progress.bytes_done,
            )
        finally:
            staged.unlink(missing_ok=True)


def load_rarfile():
    try:
        installed = version("rarfile")
    except PackageNotFoundError:
        installed = ""
    if installed != "4.5":
        install([sys.executable, "-m", "pip", "install", "--quiet", "rarfile==4.5"])
    import rarfile
    return rarfile


def prepare_rar_backend(rarfile):
    options = {"bsdtar": False, "sevenzip": False, "sevenzip2": False}
    try:
        rarfile.tool_setup(**options)
    except rarfile.RarCannotExec:
        install(["apt-get", "-qq", "update"])
        install(["apt-get", "-y", "-qq", "install", "unar"])
        rarfile.tool_setup(force=True, **options)


def install(args):
    try:
        subprocess.run(
            args, check=True, timeout=180, stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    except (OSError, subprocess.SubprocessError):
        raise ArchiveError("Colab could not install the RAR decompression support") from None


def process_archive(drive, item, input_id, output_id, processed_id, work_dir, progress):
    stem, extension = os.path.splitext(item["name"])
    if safe_parts(stem, False) != (stem,):
        raise ArchiveError("Archive has an unsafe filename", True)
    is_zip = extension.lower() == ".zip"
    with tempfile.TemporaryDirectory(dir=work_dir) as temporary:
        scratch = Path(temporary)
        path = scratch / ("archive.zip" if is_zip else "archive.rar")
        progress.phase("downloading", int(item["size"]))
        drive.download(item, path, progress.bytes_done)
        progress.phase("reading_archive")
        if is_zip:
            try:
                with zipfile.ZipFile(path) as archive:
                    extract_members(archive, True, drive, output_id, stem, scratch, progress)
            except (zipfile.BadZipFile, EOFError, UnicodeError, lzma.LZMAError, zlib.error):
                raise ArchiveError("ZIP archive is corrupt", True) from None
        else:
            rarfile = load_rarfile()
            try:
                with rarfile.RarFile(path, errors="strict") as archive:
                    if len(archive.volumelist()) != 1:
                        raise ArchiveError("Multipart RAR archives are unsupported", True)
                    if archive.needs_password():
                        raise ArchiveError("Archive is password-protected", True)
                    if any(m.compress_type != rarfile.RAR_M0 for m in archive.infolist()):
                        prepare_rar_backend(rarfile)
                    extract_members(archive, False, drive, output_id, stem, scratch, progress)
            except (rarfile.PasswordRequired, rarfile.RarWrongPassword, rarfile.NoCrypto):
                raise ArchiveError("Archive is password-protected", True) from None
            except (rarfile.BadRarFile, rarfile.NotRarFile, rarfile.RarCRCError, EOFError, UnicodeError):
                raise ArchiveError("RAR archive is corrupt", True) from None
            except rarfile.NeedFirstVolume:
                raise ArchiveError("Multipart RAR archives are unsupported", True) from None
            except rarfile.RarCannotExec:
                raise ArchiveError("Colab RAR decompression backend is unavailable") from None
    progress.phase("moving")
    drive.move_archive(item, input_id, processed_id)


def save_report(directory, report):
    temporary = directory / "report.tmp"
    temporary.write_text(json.dumps(report, ensure_ascii=True), encoding="utf-8")
    temporary.replace(directory / "report.json")


class ArchiveProgress:
    def __init__(self, directory, report, item):
        self.directory = directory
        self.report = report
        self.item = item
        self.last_write = 0

    def publish(self):
        save_report(self.directory, self.report)
        self.last_write = time.monotonic()

    def phase(self, name, total=None, member=None):
        current = {"file_id": self.item["id"], "name": self.item["name"], "phase": name}
        if total is not None:
            current.update(bytes_done=0, bytes_total=total)
        if member:
            current["member"] = member
        self.report["current_archive"] = current
        self.publish()

    def bytes_done(self, count):
        current = self.report["current_archive"]
        current["bytes_done"] = count
        if count == current["bytes_total"] or time.monotonic() - self.last_write >= 1:
            self.publish()


def run_worker(directory):
    report_file = directory / "report.json"
    if report_file.exists():
        report = json.loads(report_file.read_text(encoding="utf-8"))
        if report["done"]:
            return
        # An earlier call may have completed some archives before the kernel stopped.
    else:
        report = {"done": False, "scanned": 0, "processed": 0, "skipped": 0, "failures": [], "resolved": []}
    report.update(scan_complete=False, current_archive=None)
    save_report(directory, report)
    try:
        settings = json.loads((directory / "settings.json").read_text(encoding="utf-8"))
        drive = Drive(directory / "oauth.json")
        input_id = settings["input_folder"]
        folder = drive.get(input_id)
        if folder.get("trashed") or folder["mimeType"] != FOLDER:
            raise ArchiveError("Configured Drive input is not an active folder")
        output_id = drive.folder(input_id, "uncompressed_files")
        processed_id = drive.folder(input_id, "compressed_files")
        items = [
            item for item in drive.children(input_id)
            if ("size" in item and not item["mimeType"].startswith("application/vnd.google-apps.")
                and os.path.splitext(item["name"])[1].lower() in (".zip", ".rar"))
        ]
        known_bad = settings["bad_versions"] or {}
        pending = [
            item for item in items
            if settings["retry_failed"] or known_bad.get(item["id"]) != item["version"]
        ]
        report["scan_complete"] = True
        report["scanned"] = report["processed"] + len(items)
        report["skipped"] = len(items) - len(pending)
        report["failures"] = []
        save_report(directory, report)
        for item in sorted(pending, key=lambda item: (item["name"], item["id"])):
            progress = ArchiveProgress(directory, report, item)
            try:
                process_archive(drive, item, input_id, output_id, processed_id, directory, progress)
            except Exception as exc:
                report["failures"].append({
                    "file_id": item["id"], "version": item["version"], "name": item["name"],
                    "error": safe_detail(exc),
                    "permanent": isinstance(exc, ArchiveError) and exc.permanent,
                })
            else:
                report["processed"] += 1
                report["resolved"].append(item["id"])
            report["current_archive"] = None
            save_report(directory, report)
    except Exception as exc:
        report["error"] = safe_detail(exc)
    finally:
        report["done"] = True
        report["current_archive"] = None
        save_report(directory, report)
