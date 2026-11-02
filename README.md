# tubelens

<p align="center">
  <img src="./assets/tubelens_hero_header.png" alt="tubelens hero banner" width="700">
</p>

> **Stop Watching YouTube. Start Querying It.**

Absorb an entire YouTube channel — every video and short — in minutes, without watching a single second.

`tubelens` pulls transcripts from a whole channel, summarizes each video with a cheap model, then synthesizes everything into a thematic overview using a smart model. The result is a structured document that tells you what the creator actually argues, what frameworks they use, where they contradict themselves, and how their views have evolved over time.

```
$ python tubelens.py "https://www.youtube.com/@hubermanlab" --limit 50
[list] found 50 videos total
transcripts: 100%|████████████| 50/50 [00:42<00:00]
summaries:   100%|████████████| 48/48 [03:17<00:00]
[reduce] synthesizing chunk 1/1 with claude-sonnet-5-5
[done] wrote hubermanlab_top50_result.md
```

---

## Quick Start

For people who just want to run it.

### 1. Install

```bash
git clone https://github.com/turiya9710/tubelens.git
cd tubelens
```

Requires Python 3.10+. Create a virtual environment so `tubelens` dependencies stay isolated from your system Python:

```bash
python3 -m venv .venv
```

Activate it — **the command depends on your shell**:

```bash
source .venv/bin/activate          # bash / zsh (macOS/Linux default)
source .venv/bin/activate.csh      # tcsh / csh
source .venv/bin/activate.fish     # fish
.venv\Scripts\activate.bat         # Windows cmd
.venv\Scripts\Activate.ps1         # Windows PowerShell
```

> If you see `Badly placed ()'s.` you're in tcsh/csh — use the `.csh` variant above. The default `activate` script is bash-syntax and can't be sourced by csh-family shells.

Install dependencies:

```bash
pip install -r requirements.txt
```

When you're done with a session, `deactivate` exits the venv. Re-activate with the same command above next time.

### 2. Set your API key

`tubelens` reads `ANTHROPIC_API_KEY` from a `.env` file in the project root. **Don't paste the key into your shell history or commit it to git.**

Create `.env`:

```bash
cp .env.example .env
```

Open `.env` in your editor and paste your key:

```
ANTHROPIC_API_KEY=sk-ant-...
```

Get one at [console.anthropic.com](https://console.anthropic.com). You'll need credits — see [Cost](#cost-breakdown) below.

`.env` is already listed in `.gitignore`, so it won't be committed. Verify with `git status` before your first push.

> **Why `.env` and not `export`?**
> An `export` command lives in your shell history (`~/.bash_history`, `~/.zsh_history`) in plain text. A `.env` file stays out of version control and out of shell history. For team or CI use, swap `.env` for your platform's secret manager (GitHub Actions secrets, AWS Secrets Manager, Azure Key Vault, etc.).

**Optional — proxies.** YouTube blocks transcript requests from cloud-provider IPs and rate-limits heavy use from home IPs. If you hit `IpBlocked` or HTTP 429 errors, add Webshare rotating-residential credentials to `.env` (both are required; needs `youtube-transcript-api` v1.0+):

```
WEBSHARE_PROXY_USERNAME=...
WEBSHARE_PROXY_PASSWORD=...
```

Real environment variables override `.env`.

To upload results to Cloudflare R2, see [Cloudflare R2 upload](#cloudflare-r2-upload-optional).

### 3. Run

```bash
python tubelens.py "https://www.youtube.com/@channelname"
```

Output lands in `<channelhandle>_top<N>_result.md` (e.g. `chamath_top4_result.md`, `hubermanlab_top20_result.md`). Running with `--limit 0` produces `<handle>_all_result.md`. Open in any markdown viewer. A `<result>.manifest.json` sidecar is written next to it; re-runs use it to skip the Claude call when the videos and reduce model are unchanged.

**Default behavior:** processes up to 20 most recent videos, skips shorts, uses Claude Sonnet 5.5 for the synthesis. These defaults are tuned for fast, cheap first runs — under $0.10 for most channels. Override anything below.

### Common flags

```bash
--limit 0              # process the whole channel (default: 20 most recent)
--include-shorts       # include /shorts (default: skipped)
--output custom.md     # custom output filename (default: <handle>_top<N>_result.md or <handle>_all_result.md)
--reduce-model opus    # use Opus 5.5 for highest-quality synthesis (default: sonnet, ~5x cheaper)
--transcript-workers 2 # parallel transcript fetches (default: 8); lower if YouTube rate-limits you
```

### Single video

Pass a video URL (`watch?v=`, `youtu.be/`, `/shorts/`, `/embed/`, `/live/`) or a bare 11-character video ID instead of a channel URL. `tubelens` skips the listing and reduce steps:

```bash
python tubelens.py "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
python tubelens.py dQw4w9WgXcQ
```

Output is `<video_id>_result.md` (override with `--output`): metadata (title, URL, type, duration, uploaded date, and `GeneratedDate`, the date the file was written), a structured summary (Haiku), a detailed summary (the `--reduce-model` model), and the full transcript. Transcripts under 400 characters are not summarized. Cached in `.channel_cache/_videos/`.

### Cloudflare R2 upload (optional)

Each result file can be uploaded to a Cloudflare R2 bucket, sorted into folders by category. Add to `.env`:

```
R2_ACCOUNT_ID=...
R2_ACCESS_KEY_ID=...
R2_SECRET_ACCESS_KEY=...
R2_BUCKET=my-bucket
R2_FOLDER=tubelens
# R2_ENDPOINT_URL=...   # optional override (e.g. EU-jurisdiction buckets)
```

Create the access key as an R2 API token with Object Read & Write on the bucket. `R2_FOLDER` is the key prefix (leave it empty for the bucket root). If `R2_BUCKET` is unset, nothing is uploaded; if it is set but a credential is missing, `tubelens` says which one and skips the upload.

Files land at `<R2_FOLDER>/<Category>/<ChannelName>/<YouTubeID>_<PascalCaseName>.md`, e.g. `tubelens/Health/hubermanlab/UCxxxxxxxxxxxx_SleepAndCircadianBiology.md`:

- **ChannelName:** the channel handle (e.g. `hubermanlab`). In single-video mode it comes from the video's uploader handle, falling back to the channel title, then `UnknownChannel`. Unsafe characters become `_`.
- **YouTubeID:** the video ID in single-video mode, the channel ID (`UC…`) in channel mode.
- **PascalCaseName:** a short Haiku call names what the content is about (2–5 words, no spaces), rather than copying the video or channel title. If the call fails, the PascalCased title is used.
- **Category:** the same Haiku model picks one folder from `R2_CATEGORIES` in `tubelens.py`, based on the content. A channel gets one category, based on its whole synthesis. Anything that fits none (or a failed lookup) goes to `Uncategorized`.

| Category | Covers |
| -------- | ------ |
| `Technology` | software, programming, AI, gadgets, internet |
| `Science` | physics, biology, space, nature, research |
| `Health` | medicine, nutrition, fitness, sleep, mental health |
| `Finance` | investing, markets, economics, crypto, personal finance |
| `Business` | startups, entrepreneurship, marketing, management, careers |
| `SelfImprovement` | productivity, habits, learning, mindset, motivation |
| `Education` | tutorials, how-to, lectures, courses, explainers |
| `NewsAndPolitics` | current events, politics, policy, geopolitics, commentary |
| `HistoryAndCulture` | history, philosophy, religion, society, arts, books |
| `Entertainment` | comedy, gaming, film, music, sports, pop culture |
| `Lifestyle` | cooking, travel, home, DIY, fashion, relationships |

Edit `R2_CATEGORIES` to suit your channels; keep names PascalCase since they become folder names.

Things to know:
- Only the final result `.md` is uploaded. Transcripts, summaries and manifests stay in the local cache, and the local result file is still written.
- The name and category are cached per ID (`<id>.name.json`, `<id>.category.json`), so re-runs overwrite the same object instead of creating duplicates. Delete those files to re-pick; the object under the old key is not removed.
- An upload failure is reported but doesn't fail the run. The "no synthesis" placeholder (when no transcripts were found) is never uploaded.

That's it. Skip to [Examples](#example-output) to see what you get.

---

## Deep Dive

For people who want to understand and customize.

### How it works

<p align="center">
  <img src="./assets/tubelens_github_social_card.png" alt="tubelens pipeline: list, fetch, map, reduce" width="700">
</p>

```
┌─────────────────────────────────────────────────────────────────┐
│                      tubelens pipeline                          │
└─────────────────────────────────────────────────────────────────┘

  Channel URL
      │
      ▼
  ┌──────────────────────────────────────────────────────────┐
  │ 1. LIST       yt-dlp --flat-playlist                     │
  │               (no downloads — just enumerate IDs)        │
  │               /videos tab (/shorts: --include-shorts)    │
  └──────────────────────────────────────────────────────────┘
      │  N video metadata records
      ▼
  ┌──────────────────────────────────────────────────────────┐
  │ 2. FETCH      youtube-transcript-api (parallel, 8 threads)│
  │               Prefers manual captions over auto-generated │
  │               Cached to .channel_cache/<handle>/<id>.json│
  └──────────────────────────────────────────────────────────┘
      │  N transcripts (some may be empty if captions disabled)
      ▼
  ┌──────────────────────────────────────────────────────────┐
  │ 3. MAP        Claude Haiku 5.5 (parallel, 4 threads)     │
  │               Per-video structured summary               │
  │               Prompt-cached prefix → 90% discount        │
  │               Cached as <id>.summary.json (same folder)  │
  └──────────────────────────────────────────────────────────┘
      │  N structured summaries (THESIS / CLAIMS / MODELS / TOPICS)
      ▼
  ┌──────────────────────────────────────────────────────────┐
  │ 4. REDUCE     Sonnet 5.5 default; Opus 5.5 via flag      │
  │               8-section thematic synthesis               │
  │               Auto-chunks if total > ~150k tokens        │
  │               Reused if videos + model unchanged         │
  └──────────────────────────────────────────────────────────┘
      │
      ▼
  <handle>_top<N>_result.md  (or <handle>_all_result.md with --limit 0)
      │  (optional, if R2_BUCKET is set)
      ▼
  ┌──────────────────────────────────────────────────────────┐
  │ 5. PUBLISH    Cloudflare R2 via boto3                    │
  │               Haiku picks a name + a category folder     │
  │               <R2_FOLDER>/<Cat>/<Channel>/<ID>_<Name>.md │
  └──────────────────────────────────────────────────────────┘
```

**Why this shape:** the map step is high-volume but easy (just summarize one video), so it runs on the cheapest model with caching. The reduce step is low-volume but hard (find patterns across hundreds of summaries), so it runs on a stronger model. This is the standard map-reduce pattern adapted for LLMs.

**Why caching matters:** transcripts and summaries are cached in `.channel_cache/<handle>/` (single videos in `.channel_cache/_videos/`; the R2 name and category picks are cached there too). Re-running with a tweaked synthesis prompt costs almost nothing — only the reduce step re-executes. Iterating on the prompt becomes effectively free. Transient YouTube errors (IP blocks, 429s) are not cached, so a retry resumes where it left off. The synthesis itself is cached by `<result>.manifest.json` (video IDs + reduce model), which does not track prompt edits — delete the result `.md` to force a re-run.

### Cost breakdown

<p align="center">
  <img src="./assets/tubelens_cost_comparison.png" alt="Cost comparison between naive summarization and map-reduce" width="700">
</p>

**Default run (20 videos, Sonnet for reduce):**

| Step    | Model        | Cost          | Notes                                          |
| ------- | ------------ | ------------- | ---------------------------------------------- |
| List    | (yt-dlp)     | $0.00         | Local, no API                                  |
| Fetch   | (transcript) | $0.00         | Local, no API                                  |
| Map     | Haiku 5.5    | ~$0.03–0.05   | 20 videos, prompt caching active               |
| Reduce  | Sonnet 5.5   | ~$0.05–0.10   | Single call                                    |
| **Total** |            | **~$0.10**    | Most channels' first run lands here            |

**Full channel run (`--limit 0`, ~200 videos, Opus for reduce):**

| Step    | Model        | Cost          |
| ------- | ------------ | ------------- |
| Map     | Haiku 5.5    | ~$0.30–0.50   |
| Reduce  | Opus 5.5     | ~$0.30–0.50   |
| **Total** |            | **~$0.60–1.00** |

Re-runs (after editing the synthesis prompt) cost only the reduce step — typically $0.05–0.50 depending on model.
R2 upload adds two tiny Haiku calls (name + category) per result, once per ID — fractions of a cent.


**Token math, simplified:**
- Map step: `N × (~150 tokens prompt + ~6,000 tokens transcript + ~250 tokens output)`. The static prompt prefix is cached, billing at ~10% of normal input rate after the first call.
- Reduce step: `~600 chars × N summaries ≈ 30k tokens input + 4k tokens output`. Single call (or chunked for very large channels).

**More expensive scenarios:**
- Channels with very long videos (1hr+ podcasts): map cost scales linearly with transcript length. Consider lowering `TRANSCRIPT_TRUNCATE` in the script.
- Huge channels (summaries over ~600k chars, several hundred videos or more) trigger the chunked reduce + merge pass. Adds maybe $0.50.

### Example output

A run on a hypothetical productivity channel produces a result file (`<handle>_top20_result.md`) like:

```markdown
1. CHANNEL THESIS
This channel argues that productivity is fundamentally a problem of attention
management, not time management. The creator combines cognitive science
research with personal experimentation to challenge mainstream "hustle"
narratives, advocating instead for deep work, deliberate rest, and systems
thinking applied to personal workflows.

2. TOP 8-12 RECURRING THEMES
- Deep work vs shallow work (appears in ~40% of videos): the distinction
  between focused, cognitively demanding work and reactive busywork...
- Energy management over time management (~30%): sleep, nutrition, and
  exercise as productivity infrastructure...
- The myth of multitasking (~20%): citing Sophie Leroy's "attention residue"
  research repeatedly...
[...]

3. CORE MENTAL MODELS / FRAMEWORKS
- "Attention is the substrate of thought" — recurring framing where focus
  is treated as the limiting reagent
- Two-list system (priorities vs everything else)
- Energy as a renewable resource that requires deliberate replenishment
[...]

4. STRONGEST SPECIFIC CLAIMS
- "Switching tasks costs 23 minutes of refocus time on average" — cited
  in 4 different videos, attributed to UC Irvine research
- "I cut my email checks from 30/day to 3/day and shipped 2x more code"
  — personal experiment from "My Productivity Stack 2024"
[...]

5. EVOLUTION OVER TIME
Earlier videos (2021-2022) focused heavily on tools and apps (Notion, Roam,
Obsidian). Pivoted around mid-2023 toward systems and habits, with a
notable skepticism toward tool-switching that wasn't present earlier...

6. CONTRADICTIONS OR TENSIONS
- Advocates "single-tasking" but features productivity stacks involving
  6+ apps in workflow videos
- Says "outputs over inputs" repeatedly but tracks input metrics
  (hours read, courses completed) in self-reviews
[...]

7. WHAT'S MISSING
For a channel about productivity, surprising silence on:
- Compensation/career strategy as a productivity multiplier
- Team productivity (everything is solo-creator framed)
[...]

8. WHO SHOULD WATCH
Knowledge workers and solo creators looking for evidence-backed alternatives
to mainstream productivity advice, comfortable with reading-heavy "thinking
out loud" videos rather than tactical tutorials.
```

The format is consistent across any channel because the synthesis prompt enforces it. Run it on a finance channel and you'll get the same eight sections, just with different content.

### Customizing the synthesis prompt

This is the highest-leverage knob. The synthesis prompt lives in `tubelens.py` as `REDUCE_PROMPT`. The default extracts thesis / themes / mental models / claims / evolution / contradictions / gaps / audience.

**You can replace any section with what you actually want.** Examples:

- For a finance channel:
  > "Add a section: PORTFOLIO ALLOCATIONS the creator has actually recommended, with dates, and whether they've reversed any positions."

- For a self-help channel:
  > "Add a section: CONCRETE PROTOCOLS — every actionable practice the creator has prescribed, grouped by domain (sleep, focus, social), with dosage/duration where specified."

- For a tech channel:
  > "Add a section: TOOLS / FRAMEWORKS / PEOPLE referenced more than once, with frequency counts. Flag which ones the creator endorses vs criticizes vs just mentions."

- For a political/news channel:
  > "Add a section: FACTUAL CLAIMS that could be fact-checked, separated from opinion. Flag any claims that appear to contradict mainstream sources."

Edit `REDUCE_PROMPT` (keep the `{n}` and `{summaries}` placeholders; double any literal braces as `{{` `}}`), delete the previous result `.md` so the synthesis cache is bypassed, re-run with `--reduce-model sonnet` for cheap iteration, then switch to `opus` for the final pass. The map cache ensures only the reduce step re-runs.

**You can also customize the map prompt** (`MAP_INSTRUCTIONS` in the script) to extract different per-video signals. If you change the map prompt, delete `.channel_cache/<handle>/*.summary.json` (and the previous result `.md`) to force re-summarization. Single-video detailed summaries use `DETAILED_SUMMARY_PROMPT` (cached per video + model as `<id>.detailed.json`).

### Caveats

- **Auto-generated captions are noisy.** Filler words, mishears on jargon and proper nouns. The synthesis is robust to this in aggregate but individual claims may be slightly garbled. For channels with manual captions (rare but some creators do this), quality jumps significantly.
- **Some videos disable captions entirely.** They're skipped, as are transcripts under 400 characters. Typically <10% of a channel.
- **English captions only.** Only `en`, `en-US` and `en-GB` transcripts are fetched; other languages are skipped.
- **YouTube may block or rate-limit you.** See the proxy setup above and `--transcript-workers`.
- **Music-heavy or visual-heavy channels won't work well** — if the value is in the visuals, transcripts capture little. Don't run this on a cooking channel and expect recipes.
- **Whisper fallback isn't built in yet** but is on the roadmap. Would let you process channels without captions at the cost of audio downloads + Whisper API.

### Roadmap

Things I'd add as this gets used more:

- `--since YYYY-MM-DD` / `--until YYYY-MM-DD` — synthesize a specific time window. Useful for tracking how a creator's views shifted in a particular period.
- `--query "..."` — RAG over the transcript corpus instead of producing a synthesis. ("Has this creator ever discussed X? With what conclusion?")
- Whisper fallback for videos with no captions.
- Multi-channel comparison — run on 3 channels covering the same topic, get a synthesis of where they agree and disagree.
- HTML output with collapsible per-video summaries linked to the synthesis.
- Lightweight web UI for non-developers.
- Playlist support (currently channel or single video — playlists work the same way under the hood).

PRs welcome. Keep them focused — small, surgical changes preferred.

### License

MIT. See `LICENSE`.

### Acknowledgments

Built on top of:
- [yt-dlp](https://github.com/yt-dlp/yt-dlp) — the tireless YouTube extraction toolkit
- [youtube-transcript-api](https://github.com/jdepoix/youtube-transcript-api) — clean transcript fetching
- [boto3](https://github.com/boto/boto3) — S3-compatible uploads to Cloudflare R2
- [Anthropic Claude](https://www.anthropic.com/) — Haiku 5.5 for map, Sonnet 5.5 / Opus 5.5 for reduce