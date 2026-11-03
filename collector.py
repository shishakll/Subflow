#!/usr/bin/env python3
"""Subflow Transcript Collector: Two-Tier Resilient YouTube Subtitle Ingestion.

Architecture:
- Tier 1 (Primary): youtube-transcript-api (Ultra-fast direct API fetch, 0 media streaming)
- Tier 2 (Fallback): yt-dlp with browser cookie passthrough (--cookies-from-browser chrome)
                     activated transparently on HTTP 429, IP ban, or bot challenge.
- Idempotent: Skips videos already present in transcripts/ or marked DONE in subflow_state.db.
- Formats: Saves standard WebVTT files matching pipeline.py's expected [11charID] naming.
"""

from __future__ import annotations

import argparse
import html
import os
import random
import re
import sqlite3
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Any, List, Optional, Tuple

DB_PATH = Path("subflow_state.db")
DEFAULT_TRANSCRIPTS_DIR = Path("./transcripts")
YOUTUBE_ID_PATTERN = re.compile(r"([a-zA-Z0-9_-]{11})")


# ── 1. Video ID & Playlist Resolver ──────────────────────────────────────────
def extract_video_id(url_or_id: str) -> Optional[str]:
    """Extracts 11-character YouTube video ID from various URL formats or raw ID."""
    s = url_or_id.strip()
    if len(s) == 11 and YOUTUBE_ID_PATTERN.fullmatch(s):
        return s

    parsed = urllib.parse.urlparse(s)
    if "youtu.be" in parsed.netloc:
        vid = parsed.path.lstrip("/").split("/")[0]
        if len(vid) == 11 and YOUTUBE_ID_PATTERN.fullmatch(vid):
            return vid

    if "youtube.com" in parsed.netloc:
        qs = urllib.parse.parse_qs(parsed.query)
        if "v" in qs and len(qs["v"][0]) == 11:
            return qs["v"][0]
        if parsed.path.startswith("/shorts/") or parsed.path.startswith("/live/"):
            parts = parsed.path.strip("/").split("/")
            if len(parts) >= 2 and len(parts[1]) == 11:
                return parts[1]

    bracket_match = re.search(r"\[([a-zA-Z0-9_-]{11})\]", s)
    if bracket_match:
        return bracket_match.group(1)

    match = YOUTUBE_ID_PATTERN.search(s)
    return match.group(1) if match else None


def fetch_video_title_oembed(video_id: str) -> str:
    """Fetches the official YouTube video title via lightweight oEmbed API (0 quota, no bot block)."""
    import urllib.request
    import json

    oembed_url = f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={video_id}&format=json"
    try:
        req = urllib.request.Request(oembed_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=4) as resp:
            if resp.status == 200:
                data = json.loads(resp.read().decode("utf-8", errors="ignore"))
                title = (data.get("title") or "").strip()
                if title:
                    return title
    except Exception:
        pass
    return f"Video_{video_id}"


def resolve_video_ids_from_target(target: str) -> List[Tuple[str, str]]:
    """
    Resolves targets (URL, playlist, channel, file, or ID) into a list of (video_id, title).
    Uses yt-dlp --flat-playlist for playlist/channel discovery without downloading media.
    """
    target = target.strip()
    if not target:
        return []

    # Check if target is a local text file containing URLs
    target_path = Path(target)
    if target_path.is_file():
        results = []
        for line in target_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                results.extend(resolve_video_ids_from_target(line))
        return results

    # Check for single video ID/URL first
    single_id = extract_video_id(target)
    is_playlist_or_channel = any(kw in target for kw in ["playlist?list=", "/@", "/channel/", "/c/"])

    if single_id and not is_playlist_or_channel:
        return [(single_id, fetch_video_title_oembed(single_id))]

    # Fallback to yt-dlp --flat-playlist extraction for playlists / channels
    print(f"[탐색] 재생목록/채널 메타데이터 스캔 중: {target}")
    cmd = [
        "yt-dlp",
        "--flat-playlist",
        "--dump-single-json",
        "--no-warnings",
        target,
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
        import json
        data = json.loads(proc.stdout)
        entries = data.get("entries", [])
        resolved: List[Tuple[str, str]] = []
        if entries:
            for item in entries:
                vid = item.get("id")
                title = item.get("title") or f"Video_{vid}"
                if vid and len(vid) == 11:
                    resolved.append((vid, title))
        elif data.get("id"):
            resolved.append((data["id"], data.get("title") or f"Video_{data['id']}"))
        return resolved
    except Exception as e:
        if single_id:
            return [(single_id, f"Video_{single_id}")]
        print(f"[경고] 재생목록/채널 메타데이터 파싱 실패 ({target}): {e}")
        return []


# ── 2. Deduplication & State Check ───────────────────────────────────────────
def is_already_collected(video_id: str, output_dir: Path) -> bool:
    """Checks if a transcript for video_id already exists on disk or is DONE in SQLite."""
    # 1. Check transcripts folder
    for p in output_dir.glob(f"*[{video_id}]*.*"):
        if p.suffix.lower() in {".vtt", ".srt", ".txt"} and p.stat().st_size > 100:
            return True

    # 2. Check SQLite state DB
    if DB_PATH.exists():
        try:
            with sqlite3.connect(DB_PATH) as conn:
                cur = conn.execute("SELECT status FROM jobs WHERE video_id = ? AND status = 'DONE'", (video_id,))
                if cur.fetchone():
                    return True
        except Exception:
            pass

    return False


# ── 3. Tier 1: youtube-transcript-api ────────────────────────────────────────
def seconds_to_vtt_timestamp(seconds: float) -> str:
    """Converts seconds into WebVTT timestamp format (HH:MM:SS.mmm)."""
    hrs, remainder = divmod(seconds, 3600)
    mins, secs = divmod(remainder, 60)
    millis = int(round((secs - int(secs)) * 1000))
    return f"{int(hrs):02d}:{int(mins):02d}:{int(secs):02d}.{millis:03d}"


def serialize_to_vtt(snippets: List[Any]) -> str:
    """Serializes a list of transcript snippets into standard WebVTT format."""
    lines = ["WEBVTT", "Kind: captions", ""]
    for i, s in enumerate(snippets, start=1):
        # Support both FetchedTranscriptSnippet objects and dicts
        start = getattr(s, "start", None) or (s.get("start", 0.0) if isinstance(s, dict) else 0.0)
        dur = getattr(s, "duration", None) or (s.get("duration", 0.0) if isinstance(s, dict) else 0.0)
        text = getattr(s, "text", None) or (s.get("text", "") if isinstance(s, dict) else "")

        start_ts = seconds_to_vtt_timestamp(float(start))
        end_ts = seconds_to_vtt_timestamp(float(start) + max(0.5, float(dur)))
        clean_text = html.unescape(str(text)).replace("\n", " ").strip()

        lines.append(f"{start_ts} --> {end_ts}")
        lines.append(clean_text)
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def fetch_tier1_transcript(video_id: str) -> Optional[str]:
    """
    Tier 1 Primary Fetch: youtube-transcript-api
    Returns WebVTT string on success, or None to signal fallback to Tier 2.
    """
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
        api = YouTubeTranscriptApi()

        # Try Korean first, then English
        try:
            snippets = api.fetch(video_id, languages=["ko", "ko-KR", "en", "en-US"])
            if snippets:
                return serialize_to_vtt(list(snippets))
        except Exception:
            pass

        # Try list & translate fallback
        try:
            t_list = api.list(video_id)
            # Find any available transcript
            target_t = None
            for lang in ["ko", "en"]:
                try:
                    target_t = t_list.find_transcript([lang])
                    break
                except Exception:
                    pass

            if not target_t:
                # Pick the first available and translate to Korean if translatable
                for t in t_list:
                    if t.is_translatable:
                        try:
                            target_t = t.translate("ko")
                            break
                        except Exception:
                            pass
                    if not target_t:
                        target_t = t

            if target_t:
                snippets = target_t.fetch()
                if snippets:
                    return serialize_to_vtt(list(snippets))
        except Exception:
            pass

    except Exception as e:
        print(f"  [Tier 1 API 예외] {e}")

    return None


# ── 4. Tier 2: yt-dlp with Browser Cookies ───────────────────────────────────
def sanitize_filename(name: str) -> str:
    """Removes filesystem-unsafe characters from title."""
    cleaned = re.sub(r'[\\/*?:"<>|]', "", name)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:100] or "Transcript"


def fetch_tier2_ytdlp(
    video_id: str,
    title: str,
    output_dir: Path,
    browser: str = "chrome",
) -> Optional[Path]:
    """
    Tier 2 Fallback Fetch: yt-dlp with browser cookie passthrough
    Executes yt-dlp to download auto or manual subtitles in .vtt format.
    """
    clean_title = sanitize_filename(title)
    out_template = str(output_dir / f"{clean_title}_[{video_id}].%(ext)s")
    target_url = f"https://www.youtube.com/watch?v={video_id}"

    cmd = [
        "yt-dlp",
        "--cookies-from-browser", browser,
        "--skip-download",
        "--write-auto-sub",
        "--write-sub",
        "--sub-lang", "ko,en",
        "--sub-format", "vtt",
        "--ignore-no-formats-error",
        "-o", out_template,
        target_url,
    ]

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        # Search for created vtt file matching video_id
        for p in output_dir.glob(f"*[{video_id}]*.vtt"):
            if p.stat().st_size > 100:
                return p
        if proc.returncode != 0:
            print(f"  [Tier 2 yt-dlp 에러 로그] {proc.stderr.strip()[:200]}")
    except Exception as e:
        print(f"  [Tier 2 yt-dlp 예외] {e}")

    return None


# ── 5. Main Collector Engine ─────────────────────────────────────────────────
def collect_single_video(
    video_id: str,
    title: str,
    output_dir: Path,
    browser: str = "chrome",
    force: bool = False,
) -> bool:
    """Collects transcript for a single video using Tier 1 with Tier 2 fallback."""
    if not force and is_already_collected(video_id, output_dir):
        print(f"[건너뜀] 이미 수집된 영상: [{video_id}] {title}")
        return True

    print(f"\n[수집 시작] [{video_id}] {title}")

    # 1. Attempt Tier 1 (youtube-transcript-api)
    print("  ➔ Tier 1 (초고속 API) 시도 중...")
    vtt_content = fetch_tier1_transcript(video_id)
    if vtt_content and len(vtt_content) > 150:
        clean_title = sanitize_filename(title)
        out_file = output_dir / f"{clean_title}_[{video_id}].vtt"
        out_file.write_text(vtt_content, encoding="utf-8")
        print(f"  ✅ [Tier 1 성공] 저장 완료: {out_file.name} ({len(vtt_content):,} bytes)")
        return True

    # 2. Fallback to Tier 2 (yt-dlp + browser cookies)
    print(f"  ⚠️ Tier 1 실패(Rate limit/자막 제한) ➔ Tier 2 (yt-dlp + {browser} 쿠키) 자동 폴백...")
    out_file = fetch_tier2_ytdlp(video_id, title, output_dir, browser=browser)
    if out_file and out_file.exists():
        print(f"  ✅ [Tier 2 성공] 저장 완료: {out_file.name}")
        return True

    print(f"  ❌ [최종 실패] [{video_id}] 자막 트랙을 추출하지 못했습니다.")
    return False


def run_collector(
    targets: List[str],
    output_dir: Path = DEFAULT_TRANSCRIPTS_DIR,
    browser: str = "chrome",
    delay_min: float = 2.0,
    delay_max: float = 5.0,
    force: bool = False,
    run_pipeline_after: bool = False,
) -> None:
    """Batch collector orchestrating target resolution, rate-limit jitter, and pipeline trigger."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Resolve all targets into (video_id, title) tuples
    all_videos: List[Tuple[str, str]] = []
    seen_ids = set()

    for tgt in targets:
        for vid, title in resolve_video_ids_from_target(tgt):
            if vid not in seen_ids:
                seen_ids.add(vid)
                all_videos.append((vid, title))

    total = len(all_videos)
    if total == 0:
        print("[완료] 수집 대상 영상을 찾지 못했습니다.")
        return

    print(f"============================================================")
    print(f"🚀 Subflow Transcript Collector 시작 (총 {total}개 영상)")
    print(f"   출력 디렉터리: {output_dir.resolve()}")
    print(f"   지터 대기: {delay_min}s ~ {delay_max}s | 쿠키 브라우저: {browser}")
    print(f"============================================================")

    success_count = 0
    fail_count = 0

    for idx, (vid, title) in enumerate(all_videos, start=1):
        print(f"\n({idx}/{total}) 진행 중...")
        ok = collect_single_video(vid, title, output_dir, browser=browser, force=force)
        if ok:
            success_count += 1
        else:
            fail_count += 1

        # Inter-request random jitter to prevent IP rate limiting
        if idx < total:
            jitter = random.uniform(delay_min, delay_max)
            time.sleep(jitter)

    print("\n============================================================")
    print(f"🏁 수집 완료! 성공: {success_count}개 | 실패: {fail_count}개 (총 {total}개)")
    print("============================================================")

    # 3. Optional automatic execution of pipeline.py
    if run_pipeline_after:
        print("\n[자동 실행] 수집된 자막의 LLM 구조화 파이프라인(pipeline.py) 기동...")
        from pipeline import run_pipeline
        run_pipeline(concurrency=2)


# ── 6. CLI Entrypoint ────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Subflow Two-Tier Resilient YouTube Transcript Collector",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""예제 사용법:
  # 1. 단일 영상 수집
  python collector.py "https://www.youtube.com/watch?v=lZc30sUfx64"

  # 2. 재생목록 전체 수집
  python collector.py "https://www.youtube.com/playlist?list=PL..."

  # 3. 텍스트 파일(URL 목록) 수집
  python collector.py --file urls.txt

  # 4. 수집 후 즉시 pipeline.py 구조화 리포트 생성까지 자동 완주
  python collector.py "https://youtu.be/lZc30sUfx64" --run-pipeline
        """,
    )
    parser.add_argument("targets", nargs="*", help="유튜브 영상 URL, 재생목록 URL, 또는 11자리 비디오 ID")
    parser.add_argument("--file", "-f", help="수집할 유튜브 URL 목록 텍스트 파일 경로")
    parser.add_argument("--output-dir", "-o", default=str(DEFAULT_TRANSCRIPTS_DIR), help="자막 파일 저장 디렉터리 (기본값: ./transcripts)")
    parser.add_argument("--browser", "-b", default="chrome", help="Tier 2 fallback용 브라우저 쿠키 소스 (기본값: chrome)")
    parser.add_argument("--delay-min", type=float, default=2.0, help="요청 간 최소 랜덤 지터(초) (기본값: 2.0)")
    parser.add_argument("--delay-max", type=float, default=5.0, help="요청 간 최대 랜덤 지터(초) (기본값: 5.0)")
    parser.add_argument("--force", action="store_true", help="이미 수집된 파일도 덮어쓰고 재수집")
    parser.add_argument("--run-pipeline", action="store_true", help="수집 완료 후 즉시 pipeline.py 배치 파이프라인 자동 실행")

    args = parser.parse_args()

    targets = list(args.targets)
    if args.file:
        targets.append(args.file)

    if not targets:
        parser.print_help()
        sys.exit(1)

    run_collector(
        targets=targets,
        output_dir=Path(args.output_dir),
        browser=args.browser,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
        force=args.force,
        run_pipeline_after=args.run_pipeline,
    )


if __name__ == "__main__":
    main()
