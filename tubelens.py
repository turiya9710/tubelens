"""
tubelens — Stop Watching YouTube. Start Querying It.
====================================================
Two modes:
  - Channel mode: pulls every video + short from a channel, fetches transcripts,
    and uses Claude (map-reduce) to produce a channel-wide thematic synthesis.
  - Single-video mode: fetches transcript + summary for one video. No reduce step.
    Triggered automatically when the input is a video URL or 11-char video ID.

Pipeline (channel mode):
    1. yt-dlp           -> list all video IDs in /videos and /shorts tabs
    2. youtube-transcript-api -> fetch transcripts (skips videos w/o captions)
    3. Claude Haiku     -> per-video summary (cheap "map" step)
    4. Claude Opus      -> cross-video synthesis ("reduce" step)

Setup:
    pip install yt-dlp youtube-transcript-api anthropic tqdm
    export ANTHROPIC_API_KEY=sk-ant-...

Usage (channel):
    python tubelens.py "https://www.youtube.com/@channelname"

Usage (single video — any of these):
    python tubelens.py "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    python tubelens.py "https://youtu.be/dQw4w9WgXcQ"
    python tubelens.py "https://www.youtube.com/shorts/abc123XYZ_-"
    python tubelens.py dQw4w9WgXcQ                  # bare 11-char ID

Defaults: --limit 20, --skip-shorts, --reduce-model sonnet, output = <handle>_result.md

Override:
    python tubelens.py "https://www.youtube.com/@channelname" --limit 0          # full channel
    python tubelens.py "https://www.youtube.com/@channelname" --include-shorts   # shorts on
    python tubelens.py "https://www.youtube.com/@channelname" --reduce-model opus
    python tubelens.py "https://www.youtube.com/@channelname" --output custom.md

Cost: ~$0.50-1.00 for a 200-video channel (with prompt caching, default models).
Single-video mode is essentially free (one Haiku call, fractions of a cent).
Re-runs are free — transcripts and summaries are cached on disk.
"""

import argparse
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path

import yt_dlp
from anthropic import Anthropic
from dotenv import load_dotenv
from tqdm import tqdm
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import (
    NoTranscriptFound,
    TranscriptsDisabled,
    VideoUnavailable,
)

# WebshareProxyConfig was added in youtube-transcript-api v1.0.0. If the user is
# on an older pin, we still want the rest of the script to load — proxy support
# just becomes a no-op until they upgrade.
try:
    from youtube_transcript_api.proxies import WebshareProxyConfig
except ImportError:  # pragma: no cover
    WebshareProxyConfig = None  # type: ignore[assignment,misc]

# ---------- Models ----------

MAP_MODEL = "claude-haiku-4-5-20251001"   # cheap per-video summaries
REDUCE_MODEL = "claude-opus-4-7"          # final synthesis (best quality)
REDUCE_MODEL_CHEAP = "claude-sonnet-4-6"  # ~5x cheaper, good for iterating on prompt

MIN_TRANSCRIPT_CHARS = 400  # skip videos with near-empty transcripts (noise)
TRANSCRIPT_TRUNCATE = 25_000  # most videos repeat themselves; first 25k captures thesis

# Overwritten in main() once the channel handle is known. Kept as a module global
# so the worker functions (fetch_transcript / summarize_one) can read it without
# threading an extra arg through the ThreadPoolExecutor plumbing.
CACHE_DIR = Path(".channel_cache")


@dataclass
class Video:
    video_id: str
    title: str
    upload_date: str
    duration: int
    is_short: bool
    transcript: str = ""
    summary: str = ""
    error: str = ""


def video_url(video: Video) -> str:
    """Canonical YouTube link for a video. Shorts get the /shorts/ form so the
    link opens in the right player; regular videos use the standard watch URL."""
    if video.is_short:
        return f"https://www.youtube.com/shorts/{video.video_id}"
    return f"https://www.youtube.com/watch?v={video.video_id}"


# YouTube video IDs are exactly 11 characters from [A-Za-z0-9_-].
_VIDEO_ID_RE = re.compile(r"[A-Za-z0-9_-]{11}")
_VIDEO_URL_PATTERNS = [
    # youtu.be/<id>
    re.compile(r"youtu\.be/([A-Za-z0-9_-]{11})"),
    # youtube.com/watch?v=<id>  (also m.youtube.com, music.youtube.com)
    re.compile(r"youtube\.com/watch\?(?:[^ ]*&)?v=([A-Za-z0-9_-]{11})"),
    # youtube.com/shorts/<id>, /embed/<id>, /v/<id>, /live/<id>
    re.compile(r"youtube\.com/(?:shorts|embed|v|live)/([A-Za-z0-9_-]{11})"),
]


def extract_video_id(arg: str) -> tuple[str, bool] | None:
    """If `arg` is a YouTube video URL or a bare 11-char video ID, return
    (video_id, is_shorts_url). Otherwise return None — caller should treat the
    arg as a channel URL.

    The is_shorts hint is a soft signal from the URL only; for bare IDs we
    can't tell, and the caller will fall back to duration-based detection.
    """
    arg = arg.strip()

    # Bare 11-character ID (only — no slashes, no other length).
    # We require an exact match here, not search, to avoid false positives like
    # any 11-char substring of a channel handle.
    if "/" not in arg and "?" not in arg and len(arg) == 11 and _VIDEO_ID_RE.fullmatch(arg):
        return (arg, False)

    # Shorts URL is the only one that tells us it's a short for sure.
    is_short = "/shorts/" in arg
    for pat in _VIDEO_URL_PATTERNS:
        m = pat.search(arg)
        if m:
            return (m.group(1), is_short)
    return None


def _video_meta(video: Video) -> dict:
    """Metadata block embedded in transcript + summary cache files so the cache
    is browseable on its own (you can `cat` a file and know what video it is)."""
    return {
        "video_id": video.video_id,
        "title": video.title,
        "url": video_url(video),
        "upload_date": video.upload_date,
        "duration": video.duration,
        "is_short": video.is_short,
    }


# ---------- Step 1: list channel videos ----------

def list_channel_videos(channel_url: str, skip_shorts: bool = False) -> list[Video]:
    """Use yt-dlp's flat-playlist mode to list every video without downloading."""
    base = channel_url.rstrip("/").removesuffix("/videos").removesuffix("/shorts")
    tabs = [(f"{base}/videos", False)]
    if not skip_shorts:
        tabs.append((f"{base}/shorts", True))

    videos: list[Video] = []
    ydl_opts = {
        "quiet": True,
        "extract_flat": "in_playlist",  # walk into nested tabs but don't fetch each video
        "skip_download": True,
        "ignoreerrors": True,
    }

    def walk_entries(node, is_short_hint: bool):
        """Recursively yield video entries. yt-dlp wraps channel pages in nested
        playlist objects (Videos/Shorts/Live as sub-playlists) — we have to walk
        through them rather than assuming entries are videos directly."""
        if not node:
            return
        for entry in node.get("entries", []) or []:
            if not entry:
                continue
            entry_type = entry.get("_type") or ""
            # If this entry is itself a playlist/tab, recurse into it.
            if entry_type in ("playlist", "url") and "entries" in entry:
                # Hint: nested "Shorts" tab implies shorts
                tab_title = (entry.get("title") or "").lower()
                next_hint = is_short_hint or "short" in tab_title
                walk_entries(entry, next_hint)
                continue
            # Otherwise treat as a video entry. Need an id.
            vid = entry.get("id")
            if not vid:
                continue
            videos.append(Video(
                video_id=vid,
                title=entry.get("title", "") or "",
                upload_date=str(entry.get("upload_date", "") or ""),
                duration=int(entry.get("duration") or 0),
                is_short=is_short_hint,
            ))

    for tab_url, is_short in tabs:
        print(f"[list] scanning {tab_url}")
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(tab_url, download=False)
        except yt_dlp.utils.DownloadError as e:
            # Common, harmless: channel may not have a /shorts tab
            msg = str(e)
            if "does not have a shorts tab" in msg or "does not have a videos tab" in msg:
                print(f"[list]   (no {'shorts' if is_short else 'videos'} tab on this channel)")
            else:
                print(f"[list]   skipped ({msg[:120]})")
            continue
        except Exception as e:
            print(f"[list]   skipped ({type(e).__name__}: {e})")
            continue

        before = len(videos)
        walk_entries(info, is_short)
        print(f"[list]   found {len(videos) - before} entries in this tab")

    # Dedupe — a video can occasionally appear in both tabs
    seen = set()
    deduped = []
    for v in videos:
        if v.video_id in seen:
            continue
        seen.add(v.video_id)
        deduped.append(v)
    print(f"[list] found {len(deduped)} videos total")
    return deduped


def fetch_video_metadata(video_id: str, is_short_hint: bool = False) -> Video:
    """Fetch metadata for a single video via yt-dlp. Used by single-video mode
    where we don't have a channel listing to harvest titles/dates from.

    is_short_hint: True if the input URL was a /shorts/ URL. We trust that
    signal because it's authoritative. Without that signal, we fall back to
    duration <= 60s as a proxy (YouTube's own short threshold).
    """
    url = f"https://www.youtube.com/watch?v={video_id}"
    ydl_opts = {"quiet": True, "skip_download": True, "ignoreerrors": True}
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False) or {}
    except Exception as e:
        # If yt-dlp can't reach the video at all, return a minimal Video so the
        # rest of the pipeline can still try the transcript fetch.
        print(f"[meta] couldn't fetch metadata ({type(e).__name__}: {e}); proceeding with id only")
        return Video(
            video_id=video_id, title="", upload_date="",
            duration=0, is_short=is_short_hint,
        )

    duration = int(info.get("duration") or 0)
    is_short = is_short_hint or (0 < duration <= 60)
    return Video(
        video_id=video_id,
        title=info.get("title", "") or "",
        upload_date=str(info.get("upload_date", "") or ""),
        duration=duration,
        is_short=is_short,
    )


# ---------- Step 2: fetch transcripts ----------

# Cached YouTubeTranscriptApi instance. Rebuilt lazily on first use so the
# requests.Session it owns is reused across all worker threads (rather than
# making a fresh session — and re-reading env vars — for each video).
_TRANSCRIPT_API: YouTubeTranscriptApi | None = None
_TRANSCRIPT_API_LOCK = __import__("threading").Lock()


def _get_transcript_api() -> YouTubeTranscriptApi:
    """Return a process-wide YouTubeTranscriptApi, configured with Webshare
    proxies when WEBSHARE_PROXY_USERNAME / WEBSHARE_PROXY_PASSWORD are set.

    YouTube blocks transcript requests from cloud-provider IPs and aggressively
    rate-limits residential IPs that hit it too often. The library author
    recommends Webshare's rotating residential proxies for this; it's a few
    dollars a month and the only reliable workaround for IpBlocked errors.
    Sign up at https://www.webshare.io and copy the proxy username/password
    from the dashboard into your .env file.
    """
    global _TRANSCRIPT_API
    if _TRANSCRIPT_API is not None:
        return _TRANSCRIPT_API
    with _TRANSCRIPT_API_LOCK:
        # Re-check inside the lock — another thread may have built it while we waited
        if _TRANSCRIPT_API is not None:
            return _TRANSCRIPT_API

        ws_user = os.environ.get("WEBSHARE_PROXY_USERNAME")
        ws_pass = os.environ.get("WEBSHARE_PROXY_PASSWORD")

        if ws_user and ws_pass and WebshareProxyConfig is not None:
            print("[transcripts] using Webshare rotating residential proxies")
            _TRANSCRIPT_API = YouTubeTranscriptApi(
                proxy_config=WebshareProxyConfig(
                    proxy_username=ws_user,
                    proxy_password=ws_pass,
                ),
            )
        else:
            if ws_user or ws_pass:
                # User set one but not both — almost certainly a config mistake worth flagging
                print(
                    "[transcripts] WARNING: WEBSHARE_PROXY_USERNAME/PASSWORD must both "
                    "be set to enable proxy. Falling back to direct connection."
                )
            elif WebshareProxyConfig is None and (ws_user or ws_pass):
                print(
                    "[transcripts] WARNING: youtube-transcript-api is too old for "
                    "WebshareProxyConfig. Run: pip install -U youtube-transcript-api"
                )
            _TRANSCRIPT_API = YouTubeTranscriptApi()
        return _TRANSCRIPT_API


def fetch_transcript(video: Video) -> Video:
    """Fetch transcript with on-disk caching. Prefers manual captions over auto.

    Uses youtube-transcript-api v1.0+ instance API (list_transcripts was removed).
    """
    cache_file = CACHE_DIR / f"{video.video_id}.json"
    if cache_file.exists():
        data = json.loads(cache_file.read_text())
        video.transcript = data.get("transcript", "")
        video.error = data.get("error", "")
        return video

    try:
        ytt_api = _get_transcript_api()
        transcript_list = ytt_api.list(video.video_id)

        # Prefer English manual captions; fall back to auto-generated.
        try:
            tr = transcript_list.find_manually_created_transcript(["en", "en-US", "en-GB"])
        except NoTranscriptFound:
            tr = transcript_list.find_generated_transcript(["en", "en-US", "en-GB"])

        fetched = tr.fetch()
        # FetchedTranscript has a .snippets attribute (list of FetchedTranscriptSnippet
        # objects with .text). It's also iterable directly.
        snippets = fetched.snippets if hasattr(fetched, "snippets") else fetched
        video.transcript = " ".join(
            (s.text if hasattr(s, "text") else s["text"]).replace("\n", " ")
            for s in snippets
        ).strip()
    except (TranscriptsDisabled, NoTranscriptFound, VideoUnavailable) as e:
        video.error = type(e).__name__
    except Exception as e:
        video.error = f"unexpected: {type(e).__name__}: {e}"

    if not _is_transient_error(video.error):
        cache_file.write_text(json.dumps({
            **_video_meta(video),
            "transcript": video.transcript,
            "error": video.error,
        }, indent=2, ensure_ascii=False))
    return video


# Errors we deliberately do NOT cache — they signal "YouTube is rate-limiting or
# blocking right now," which is a temporary condition. Caching them would mean
# a user who fixes the underlying issue (slows down, switches IPs, sets up
# Webshare, waits out the cooldown) would still see SKIP for these videos
# until they manually wipe the cache.
#   - IpBlocked: YouTube has blacklisted the source IP outright
#   - RequestBlocked: rate-limited at the request level
#   - 429: HTTP rate limit (sometimes wraps the above; sometimes appears alone)
#   - RetryError: requests/urllib3's wrapper when retries get exhausted, often
#     because of cascading 429s — also transient
# Successful transcripts and *real* errors (TranscriptsDisabled, NoTranscriptFound,
# VideoUnavailable) are still cached because they won't change on retry.
_TRANSIENT_ERROR_MARKERS = ("IpBlocked", "RequestBlocked", "RetryError", "429")


def _is_transient_error(error: str) -> bool:
    return bool(error) and any(m in error for m in _TRANSIENT_ERROR_MARKERS)


def fetch_all_transcripts(videos: list[Video], workers: int = 8) -> list[Video]:
    """Parallel transcript fetch — network-bound, so threads are fine."""
    out: list[Video] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch_transcript, v): v for v in videos}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="transcripts"):
            out.append(fut.result())
    got = sum(1 for v in out if v.transcript)
    print(f"[transcripts] {got}/{len(out)} videos have transcripts")

    # If anything failed, summarize why — silent failures are the worst kind of bug
    failures = [v for v in out if not v.transcript and v.error]
    if failures:
        from collections import Counter
        reasons = Counter(v.error.split(":")[0] for v in failures)
        print(f"[transcripts] failure breakdown: {dict(reasons)}")
        # Show one example of each unique error verbatim, for debugging
        seen_keys = set()
        for v in failures:
            key = v.error.split(":")[0]
            if key in seen_keys:
                continue
            seen_keys.add(key)
            print(f"[transcripts]   example ({v.video_id}): {v.error}")

        # Actionable hint based on what kind of failure dominated. The proxy might
        # ALSO get blocked / rate-limited, but in that case the user already knows
        # what Webshare is — so we only pitch it to people who aren't using it yet.
        ip_blocked = any("IpBlocked" in v.error for v in failures)
        rate_limited = any(
            ("429" in v.error or "RetryError" in v.error or "RequestBlocked" in v.error)
            for v in failures
        )
        using_webshare = bool(
            os.environ.get("WEBSHARE_PROXY_USERNAME")
            and os.environ.get("WEBSHARE_PROXY_PASSWORD")
        )
        if not using_webshare:
            if ip_blocked:
                # Hard block — slowing down won't help; need a different IP.
                print(
                    "[transcripts] HINT: YouTube has blocked your IP outright. "
                    "The fix is rotating residential proxies — sign up at "
                    "https://webshare.io, get the 'Rotating Residential' plan "
                    "(~$3.50/mo for 1GB), then add to .env:\n"
                    "    WEBSHARE_PROXY_USERNAME=...\n"
                    "    WEBSHARE_PROXY_PASSWORD=...\n"
                    "    (find both at https://dashboard.webshare.io/proxy/settings)"
                )
            elif rate_limited:
                # Soft rate-limit — try to slow down before paying for proxies.
                # Re-runs are cheap because successes are cached and these
                # transient errors aren't, so each pass fills more of the gap.
                print(
                    "[transcripts] HINT: YouTube is rate-limiting (HTTP 429). "
                    "Try slowing down — re-run with --transcript-workers 1 and "
                    "wait a few minutes between runs. These errors aren't "
                    "cached, so each retry resumes where you left off. If that "
                    "doesn't clear it, set WEBSHARE_PROXY_USERNAME/PASSWORD in "
                    ".env (rotating residential, ~$3.50/mo)."
                )
    return out


# ---------- Step 3: per-video summary (map) ----------

# Static instructions — cached across all map calls (90% discount on this prefix).
MAP_INSTRUCTIONS = """Summarize this video for cross-channel synthesis. Format exactly:

THESIS: 1 sentence — what is the creator arguing or teaching?
CLAIMS:
- 3-5 bullets, each with specifics (numbers, names, evidence)
MODELS: frameworks, analogies, or recurring concepts
TOPICS: 3-5 short tags

Terse. No filler. Write SKIP if transcript is too short or off-topic."""


def _map_user_content(video: Video, transcript: str) -> list[dict]:
    """Build a 2-block message: cached static instructions + per-video data."""
    fmt = "Short" if video.is_short else "Video"
    return [
        {
            "type": "text",
            "text": MAP_INSTRUCTIONS,
            "cache_control": {"type": "ephemeral"},
        },
        {
            "type": "text",
            "text": f"{video.title} ({video.upload_date}, {fmt}):\n{transcript}",
        },
    ]


def summarize_one(client: Anthropic, video: Video) -> Video:
    cache_file = CACHE_DIR / f"{video.video_id}.summary.json"
    if cache_file.exists():
        data = json.loads(cache_file.read_text())
        video.summary = data.get("summary", "")
        return video
    if not video.transcript or len(video.transcript) < MIN_TRANSCRIPT_CHARS:
        return video

    transcript = video.transcript[:TRANSCRIPT_TRUNCATE]

    for attempt in range(3):
        try:
            resp = client.messages.create(
                model=MAP_MODEL,
                max_tokens=400,
                messages=[{
                    "role": "user",
                    "content": _map_user_content(video, transcript),
                }],
            )
            video.summary = resp.content[0].text.strip()
            cache_file.write_text(json.dumps({
                **_video_meta(video),
                "summary": video.summary,
            }, indent=2, ensure_ascii=False))
            return video
        except Exception as e:
            if attempt == 2:
                video.error = f"map failed: {e}"
                return video
            time.sleep(2 ** attempt)
    return video


def summarize_all(videos: list[Video], workers: int = 4) -> list[Video]:
    client = Anthropic()
    have_transcript = [v for v in videos if v.transcript]
    out_map = {v.video_id: v for v in videos}

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(summarize_one, client, v): v for v in have_transcript}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="summaries"):
            v = fut.result()
            out_map[v.video_id] = v

    return list(out_map.values())


# ---------- Step 4: cross-channel synthesis (reduce) ----------

REDUCE_PROMPT = """You are synthesizing an entire YouTube channel's content into a thematic overview.

Below are per-video summaries from {n} videos/shorts on the channel, ordered roughly by upload date.

{summaries}

Produce a synthesis with these sections:

1. CHANNEL THESIS — In 2-3 sentences, what is this channel fundamentally about? What worldview or expertise does the creator offer?

2. TOP 8-12 RECURRING THEMES — Themes that appear across multiple videos. For each: name it, give 1-2 sentence description, and note ~how often it comes up.

3. CORE MENTAL MODELS / FRAMEWORKS — The creator's signature ways of thinking. Frameworks, analogies, or distinctions they return to repeatedly.

4. STRONGEST SPECIFIC CLAIMS — Concrete, falsifiable, or evidence-backed claims (not vague opinions). Pull the most substantive ones with attribution to the video title.

5. EVOLUTION OVER TIME — If you can detect it from the dates: how has the creator's focus, tone, or position shifted? Note any notable pivots.

6. CONTRADICTIONS OR TENSIONS — Places where the creator seems to argue different things in different videos, or where their stated framework doesn't match their advice.

7. WHAT'S MISSING — Topics you'd expect them to cover given their stated thesis but that don't show up.

8. WHO SHOULD WATCH — In one sentence, who is the ideal viewer.

Be specific. Reference video titles where relevant. Avoid generic observations that could apply to any channel."""


def _synthesis_fingerprint(video_ids: list[str], model: str) -> str:
    """Stable hash of the summarized-video set + model. If this matches the
    cached manifest, the synthesis output is still valid and can be reused."""
    import hashlib
    key = model + "|" + ",".join(sorted(video_ids))
    return hashlib.md5(key.encode()).hexdigest()


def synthesize(videos: list[Video], output_path: Path, model: str = REDUCE_MODEL) -> str:
    summarized = [v for v in videos if v.summary and v.summary != "SKIP"]
    summarized.sort(key=lambda v: v.upload_date)

    # Log visibility into what didn't make the cut — silently dropped videos
    # are the hardest bugs to notice.
    skipped_no_transcript = [v for v in videos if not v.transcript]
    skipped_no_summary = [
        v for v in videos
        if v.transcript and (not v.summary or v.summary == "SKIP")
    ]
    if skipped_no_transcript:
        print(
            f"[reduce] {len(skipped_no_transcript)} video(s) excluded — no transcript: "
            + ", ".join(v.video_id for v in skipped_no_transcript)
        )
    if skipped_no_summary:
        print(
            f"[reduce] {len(skipped_no_summary)} video(s) excluded — summary skipped "
            f"(transcript too short or model returned SKIP): "
            + ", ".join(v.video_id for v in skipped_no_summary)
        )

    if not summarized:
        msg = (
            "[reduce] no per-video summaries to synthesize — skipping the reduce step.\n"
            "         (this usually means transcripts couldn't be fetched; see "
            "the [transcripts] failure breakdown above)"
        )
        print(msg)
        output_path.write_text(
            "# tubelens — no synthesis produced\n\n"
            "No video transcripts were available, so there was nothing to summarize.\n\n"
            "Common causes:\n"
            "- The channel disables auto-captions on its videos\n"
            "- youtube-transcript-api is being rate-limited or IP-blocked\n"
            "- A library version mismatch (run: pip install -U youtube-transcript-api)\n"
        )
        return msg

    # Cache check — if the exact same set of videos (same IDs, same model) was
    # already synthesized and the output file still exists, skip the Claude call
    # entirely. The reduce step costs money and takes ~2 min; re-running it for
    # zero new summaries is pure waste.
    manifest_file = output_path.with_suffix(".manifest.json")
    fingerprint = _synthesis_fingerprint([v.video_id for v in summarized], model)
    if output_path.exists() and manifest_file.exists():
        try:
            manifest = json.loads(manifest_file.read_text())
            if manifest.get("fingerprint") == fingerprint:
                cached = output_path.read_text()
                print(
                    f"[reduce] {len(summarized)} summaries unchanged since last synthesis "
                    f"— returning cached result (delete {output_path.name} to force re-run)"
                )
                return cached
        except Exception:
            pass  # Corrupt manifest — just re-run

    blocks = []
    for v in summarized:
        fmt = "SHORT" if v.is_short else "VIDEO"
        blocks.append(
            f"--- [{v.upload_date}] [{fmt}] {v.title}\n{v.summary}"
        )
    joined = "\n\n".join(blocks)

    # Opus has a large context window, but if a channel is huge we still
    # want to chunk. Roughly: 1 token ~ 4 chars; cap input around 150k tokens.
    MAX_CHARS = 600_000
    if len(joined) <= MAX_CHARS:
        chunks = [joined]
    else:
        # Greedy chunking on summary boundaries
        chunks, current, size = [], [], 0
        for b in blocks:
            if size + len(b) > MAX_CHARS and current:
                chunks.append("\n\n".join(current))
                current, size = [], 0
            current.append(b)
            size += len(b)
        if current:
            chunks.append("\n\n".join(current))
        print(f"[reduce] channel too large for one pass — splitting into {len(chunks)} chunks")

    client = Anthropic()
    partials = []
    for i, chunk in enumerate(chunks, 1):
        print(f"[reduce] synthesizing chunk {i}/{len(chunks)} with {model}")
        resp = client.messages.create(
            model=model,
            max_tokens=16000,
            messages=[{
                "role": "user",
                "content": REDUCE_PROMPT.format(n=len(summarized), summaries=chunk),
            }],
        )
        partials.append(resp.content[0].text)

    if len(partials) == 1:
        final = partials[0]
    else:
        # Second-pass synthesis of chunk-level syntheses
        print("[reduce] merging chunk syntheses")
        merge_prompt = (
            "Below are partial syntheses of different time-slices of one YouTube channel. "
            "Merge them into a single synthesis using the same 8-section structure. "
            "Resolve duplicates, preserve specifics.\n\n"
            + "\n\n=====\n\n".join(partials)
        )
        resp = client.messages.create(
            model=model,
            max_tokens=16000,
            messages=[{"role": "user", "content": merge_prompt}],
        )
        final = resp.content[0].text

    output_path.write_text(final)
    # Write manifest so the next run can detect an unchanged summary set and
    # skip the Claude call entirely.
    manifest_file.write_text(json.dumps({
        "fingerprint": fingerprint,
        "video_count": len(summarized),
        "model": model,
        "video_ids": sorted(v.video_id for v in summarized),
    }, indent=2))
    return final


def process_single_video(video_id: str, is_short_hint: bool, output_path: Path) -> str:
    """Single-video pipeline: metadata -> transcript -> summary. No reduce step.

    Writes a markdown file with the metadata, summary, and full transcript so
    the user has everything in one place, and prints the summary to stdout.
    Returns the summary text.
    """
    print(f"[meta] fetching metadata for {video_id}")
    video = fetch_video_metadata(video_id, is_short_hint=is_short_hint)
    if video.title:
        print(f"[meta] {video.title}")

    print("[transcripts] fetching transcript")
    video = fetch_transcript(video)

    if not video.transcript:
        print(f"[transcripts] failed: {video.error or 'no transcript available'}")
        # Mirror the failure hints that fetch_all_transcripts gives in channel
        # mode, so single-video runs aren't a worse debugging experience.
        using_webshare = bool(
            os.environ.get("WEBSHARE_PROXY_USERNAME")
            and os.environ.get("WEBSHARE_PROXY_PASSWORD")
        )
        if video.error and not using_webshare:
            if "IpBlocked" in video.error:
                print(
                    "[transcripts] HINT: YouTube has blocked your IP. Wait a few "
                    "hours, switch networks (e.g. phone hotspot), or set "
                    "WEBSHARE_PROXY_USERNAME/PASSWORD in .env for rotating "
                    "residential proxies."
                )
            elif "429" in video.error or "RetryError" in video.error or "RequestBlocked" in video.error:
                print(
                    "[transcripts] HINT: YouTube is rate-limiting your IP. Wait "
                    "a few minutes and re-run — this error isn't cached. If it "
                    "persists, set WEBSHARE_PROXY_USERNAME/PASSWORD in .env."
                )
        output_path.write_text(
            f"# {video.title or video_id}\n\n"
            f"**URL:** {video_url(video)}\n\n"
            f"## No transcript available\n\n"
            f"Error: `{video.error or 'unknown'}`\n"
        )
        return ""

    print(f"[transcripts] got transcript ({len(video.transcript):,} chars)")

    if len(video.transcript) < MIN_TRANSCRIPT_CHARS:
        print(f"[summary] transcript too short ({len(video.transcript)} chars), skipping summary")
        summary = ""
    else:
        print("[summary] generating summary")
        client = Anthropic()
        video = summarize_one(client, video)
        summary = video.summary

    # Build a self-contained markdown report. Order: metadata, summary, then
    # transcript at the bottom (long, mostly for reference).
    duration_str = (
        f"{video.duration // 60}m {video.duration % 60}s"
        if video.duration else "unknown"
    )
    fmt = "Short" if video.is_short else "Video"
    parts = [
        f"# {video.title or video_id}",
        "",
        f"**URL:** {video_url(video)}  ",
        f"**Type:** {fmt}  ",
        f"**Duration:** {duration_str}  ",
        f"**Uploaded:** {video.upload_date or 'unknown'}",
        "",
    ]
    if summary:
        parts += ["## Summary", "", summary, ""]
    parts += ["## Transcript", "", video.transcript, ""]
    output_path.write_text("\n".join(parts))
    return summary


def _channel_handle(channel_url: str) -> str:
    """Extract a filesystem-safe handle from a channel URL.

    e.g. https://www.youtube.com/@hubermanlab/videos -> 'hubermanlab'
         https://www.youtube.com/c/SomeChannel       -> 'SomeChannel'
    Falls back to 'channel' if nothing sensible can be parsed.
    """
    cleaned = re.sub(r"[?#].*$", "", channel_url.rstrip("/"))
    cleaned = cleaned.removesuffix("/videos").removesuffix("/shorts")
    handle = cleaned.rsplit("/", 1)[-1] or "channel"
    handle = handle.lstrip("@") or "channel"
    return re.sub(r"[^A-Za-z0-9_.-]", "_", handle)


def _derive_output_path(
    channel_url: str,
    explicit: str | None,
    processed_count: int,
    limit_arg: int,
) -> Path:
    """Default output filename: <handle>_top<N>_result.md (or _all_ if --limit 0).

    Uses the actual processed count for accuracy. Uses 'all' only when the user
    explicitly opted into the full channel with --limit 0, so users can tell
    "I asked for everything" runs apart from "the channel happens to be small."

    Examples:
        @chamath (4 vids, default limit=20)  -> chamath_top4_result.md
        @hubermanlab --limit 50              -> hubermanlab_top50_result.md
        @hubermanlab --limit 0  (300 videos) -> hubermanlab_all_result.md
        @chamath --limit 0      (4 videos)   -> chamath_all_result.md
    """
    if explicit:
        return Path(explicit)

    handle = _channel_handle(channel_url)

    # 'all' only when the user explicitly asked for the full channel (--limit 0).
    # Otherwise reflect what was actually processed.
    scope = "all" if limit_arg == 0 else f"top{processed_count}"
    return Path(f"{handle}_{scope}_result.md")


# ---------- main ----------

def main():
    ap = argparse.ArgumentParser(
        description="tubelens — Stop watching YouTube. Start querying it.",
    )
    ap.add_argument(
        "channel_url",
        metavar="URL_OR_ID",
        help="Channel URL (e.g. https://www.youtube.com/@channelname) for full-channel "
             "synthesis, OR a video URL / 11-char video ID to summarize a single video.",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=20,
        help="cap number of videos to process (default: 20). Use --limit 0 for the full channel.",
    )
    # Shorts off by default. Users opt in with --include-shorts.
    shorts_group = ap.add_mutually_exclusive_group()
    shorts_group.add_argument(
        "--skip-shorts",
        dest="skip_shorts",
        action="store_true",
        default=True,
        help="ignore the /shorts tab (default)",
    )
    shorts_group.add_argument(
        "--include-shorts",
        dest="skip_shorts",
        action="store_false",
        help="include /shorts in the synthesis",
    )
    ap.add_argument(
        "--output",
        default=None,
        help="output filename (default: <channelhandle>_result.md)",
    )
    ap.add_argument(
        "--reduce-model",
        choices=["opus", "sonnet"],
        default="sonnet",
        help="sonnet = ~5x cheaper (default, good quality), opus = best quality for final runs",
    )
    ap.add_argument(
        "--transcript-workers",
        type=int,
        default=8,
        help="parallel transcript fetches (default: 8). Lower to 1-2 if YouTube is "
             "rate-limiting / IP-blocking your connection — slower but less likely to trip flags.",
    )
    args = ap.parse_args()

    # Load ANTHROPIC_API_KEY from .env if present. Existing env vars take precedence
    # (so CI/secret managers override the .env file without surprise).
    load_dotenv()

    if not os.environ.get("ANTHROPIC_API_KEY"):
        sys.exit(
            "ANTHROPIC_API_KEY not found.\n"
            "Copy .env.example to .env and add your key, "
            "or set ANTHROPIC_API_KEY in your environment."
        )

    reduce_model = REDUCE_MODEL if args.reduce_model == "opus" else REDUCE_MODEL_CHEAP

    # CACHE_DIR is set differently for single-video vs channel mode below;
    # declared here once so both branches can assign without re-stating `global`.
    global CACHE_DIR

    # Single-video shortcut: if the positional arg parses as a video URL/ID,
    # skip the channel listing + reduce pipeline and just produce one summary.
    # Cache lives under .channel_cache/_videos/ since there's no channel handle
    # (the leading underscore can't appear in a YouTube handle, so it can't
    # collide with a real channel folder).
    video_match = extract_video_id(args.channel_url)
    if video_match is not None:
        video_id, is_short_hint = video_match
        CACHE_DIR = Path(".channel_cache") / "_videos"
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        print(f"[cache] using {CACHE_DIR}/")

        out_path = Path(args.output) if args.output else Path(f"{video_id}_result.md")
        summary = process_single_video(video_id, is_short_hint, out_path)
        print(f"\n[done] wrote {out_path}")
        if summary:
            print("\n" + "=" * 60)
            print(summary)
        return

    # Per-channel cache dir, e.g. ".channel_cache/hubermanlab/" — keeps cached
    # transcripts and summaries from different channels from mingling, while
    # grouping every channel's cache under one hidden top-level folder.
    # Must be set before any worker reads it.
    CACHE_DIR = Path(".channel_cache") / _channel_handle(args.channel_url)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[cache] using {CACHE_DIR}/")

    videos = list_channel_videos(args.channel_url, skip_shorts=args.skip_shorts)
    total_found = len(videos)
    if args.limit and total_found > args.limit:
        videos = videos[: args.limit]
        print(
            f"[limit] processing first {args.limit} of {total_found} videos. "
            f"Use --limit 0 to process the whole channel."
        )
    if not videos:
        sys.exit("No videos found.")

    videos = fetch_all_transcripts(videos, workers=args.transcript_workers)
    videos = summarize_all(videos)

    out_path = _derive_output_path(args.channel_url, args.output, len(videos), args.limit)
    final = synthesize(videos, out_path, model=reduce_model)
    print(f"\n[done] wrote {out_path}")
    if len(final) > 4000:
        print(f"\n{'=' * 60} preview (full output in {out_path}) {'=' * 60}")
        print(final[:4000])
        print(f"\n... ({len(final):,} chars total — open {out_path} for the complete synthesis)")
    else:
        print("\n" + "=" * 60)
        print(final)


if __name__ == "__main__":
    main()