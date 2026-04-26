"""
tubelens — Stop Watching YouTube. Start Querying It.
====================================================
Pulls every video + short from a channel, fetches transcripts, and uses
Claude (map-reduce) to produce a channel-wide thematic synthesis.

Pipeline:
    1. yt-dlp           -> list all video IDs in /videos and /shorts tabs
    2. youtube-transcript-api -> fetch transcripts (skips videos w/o captions)
    3. Claude Haiku     -> per-video summary (cheap "map" step)
    4. Claude Opus      -> cross-video synthesis ("reduce" step)

Setup:
    pip install yt-dlp youtube-transcript-api anthropic tqdm
    export ANTHROPIC_API_KEY=sk-ant-...

Usage:
    python tubelens.py "https://www.youtube.com/@channelname"
    python tubelens.py "https://www.youtube.com/@channelname" --limit 50
    python tubelens.py "https://www.youtube.com/@channelname" --skip-shorts
    python tubelens.py "https://www.youtube.com/@channelname" --reduce-model sonnet

Cost: ~$0.50-1.00 for a 200-video channel (with prompt caching, default models).
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
from tqdm import tqdm
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import (
    NoTranscriptFound,
    TranscriptsDisabled,
    VideoUnavailable,
)

# ---------- Models ----------

MAP_MODEL = "claude-haiku-4-5-20251001"   # cheap per-video summaries
REDUCE_MODEL = "claude-opus-4-7"          # final synthesis (best quality)
REDUCE_MODEL_CHEAP = "claude-sonnet-4-6"  # ~5x cheaper, good for iterating on prompt

MIN_TRANSCRIPT_CHARS = 400  # skip videos with near-empty transcripts (noise)
TRANSCRIPT_TRUNCATE = 25_000  # most videos repeat themselves; first 25k captures thesis

CACHE_DIR = Path(".channel_cache")
CACHE_DIR.mkdir(exist_ok=True)


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
        "extract_flat": True,
        "skip_download": True,
        "ignoreerrors": True,
    }

    for tab_url, is_short in tabs:
        print(f"[list] scanning {tab_url}")
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(tab_url, download=False)
        except Exception as e:
            print(f"[list]   skipped ({e})")
            continue

        for entry in (info or {}).get("entries", []) or []:
            if not entry or not entry.get("id"):
                continue
            videos.append(Video(
                video_id=entry["id"],
                title=entry.get("title", "") or "",
                upload_date=str(entry.get("upload_date", "") or ""),
                duration=int(entry.get("duration") or 0),
                is_short=is_short,
            ))

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


# ---------- Step 2: fetch transcripts ----------

def fetch_transcript(video: Video) -> Video:
    """Fetch transcript with on-disk caching. Prefers manual captions over auto."""
    cache_file = CACHE_DIR / f"{video.video_id}.json"
    if cache_file.exists():
        data = json.loads(cache_file.read_text())
        video.transcript = data.get("transcript", "")
        video.error = data.get("error", "")
        return video

    try:
        # Prefer English manual captions; fall back to auto-generated.
        transcript_list = YouTubeTranscriptApi.list_transcripts(video.video_id)
        try:
            tr = transcript_list.find_manually_created_transcript(["en", "en-US", "en-GB"])
        except NoTranscriptFound:
            tr = transcript_list.find_generated_transcript(["en"])
        chunks = tr.fetch()
        video.transcript = " ".join(c["text"].replace("\n", " ") for c in chunks).strip()
    except (TranscriptsDisabled, NoTranscriptFound, VideoUnavailable) as e:
        video.error = type(e).__name__
    except Exception as e:
        video.error = f"unexpected: {e}"

    cache_file.write_text(json.dumps({
        "transcript": video.transcript,
        "error": video.error,
    }))
    return video


def fetch_all_transcripts(videos: list[Video], workers: int = 8) -> list[Video]:
    """Parallel transcript fetch — network-bound, so threads are fine."""
    out: list[Video] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch_transcript, v): v for v in videos}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="transcripts"):
            out.append(fut.result())
    got = sum(1 for v in out if v.transcript)
    print(f"[transcripts] {got}/{len(out)} videos have transcripts")
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
    cache_file = CACHE_DIR / f"{video.video_id}.summary.txt"
    if cache_file.exists():
        video.summary = cache_file.read_text()
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
            cache_file.write_text(video.summary)
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


def synthesize(videos: list[Video], output_path: Path, model: str = REDUCE_MODEL) -> str:
    summarized = [v for v in videos if v.summary and v.summary != "SKIP"]
    summarized.sort(key=lambda v: v.upload_date)

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
            max_tokens=4000,
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
            max_tokens=4000,
            messages=[{"role": "user", "content": merge_prompt}],
        )
        final = resp.content[0].text

    output_path.write_text(final)
    return final


# ---------- main ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("channel_url", help="e.g. https://www.youtube.com/@channelname")
    ap.add_argument("--limit", type=int, default=0, help="cap number of videos (0 = all)")
    ap.add_argument("--skip-shorts", action="store_true")
    ap.add_argument("--output", default="channel_synthesis.md")
    ap.add_argument(
        "--reduce-model",
        choices=["opus", "sonnet"],
        default="opus",
        help="opus = best quality (default), sonnet = ~5x cheaper for iterating on the synthesis prompt",
    )
    args = ap.parse_args()

    if not os.environ.get("ANTHROPIC_API_KEY"):
        sys.exit("Set ANTHROPIC_API_KEY in your environment.")

    reduce_model = REDUCE_MODEL if args.reduce_model == "opus" else REDUCE_MODEL_CHEAP

    videos = list_channel_videos(args.channel_url, skip_shorts=args.skip_shorts)
    if args.limit:
        videos = videos[: args.limit]
    if not videos:
        sys.exit("No videos found.")

    videos = fetch_all_transcripts(videos)
    videos = summarize_all(videos)

    out_path = Path(args.output)
    final = synthesize(videos, out_path, model=reduce_model)
    print(f"\n[done] wrote {out_path}")
    print("\n" + "=" * 60)
    print(final[:2000])
    print("..." if len(final) > 2000 else "")


if __name__ == "__main__":
    main()
