# Darija Dataset Builder

A multi-source pipeline for building large-scale **Darija (Moroccan Arabic)** NLP datasets. Collects, cleans, and validates comments from 5 platforms, producing a high-quality dataset ready for machine learning.

**Dataset on Hugging Face:** [IlyasFardaouixx/moroccan-darija-dataset](https://huggingface.co/datasets/IlyasFardaouixx/moroccan-darija-dataset) — 130,851 clean Darija comments.

## Features

- **5 scrapers**: YouTube (API v3), Hespress (web), TikTok (web), Twitter/X (API v2), Facebook (Selenium)
- **Language detection**: FastText + Darija-specific word bank (200+ words, Arabic + Latin script)
- **Data cleaning**: HTML removal, emoji stripping, Unicode normalization, foreign language rejection
- **Latin Darija support**: Preserves Darija written in Latin script with number phonetics (3=ع, 7=ح, 9=ق)
- **Synthetic generator**: 564 base comments across 18 themes with 7 variation strategies
- **Central merge pipeline**: Fuses all sources with deduplication and quality filtering
- **Performance optimizations**: FastText caching, batch processing, pre-compiled regex

## Architecture

```
darija_dataset/
├── config/
│   ├── config.py              # Global settings (cleaning, detection, scraping)
│   └── advanced_config.py     # Performance profiles (S/M/L/XL)
├── src/
│   ├── cleaner.py             # Text cleaning (URLs, HTML, emojis, special chars)
│   ├── language_detector.py   # FastText + Darija heuristics hybrid detection
│   ├── darija_wordbank.py     # 200+ Darija words (Arabic + Latin/Arabizi)
│   ├── darija_dataset_generator.py  # Synthetic dataset generator (18 themes)
│   ├── csv_manager.py         # CSV I/O with deduplication
│   ├── pipeline.py            # Core processing pipeline
│   ├── merge_pipeline.py      # Multi-source merge + dashboard
│   ├── youtube_scraper.py     # YouTube Data API v3
│   ├── hespress_scraper.py    # Hespress news comments
│   ├── tiktok_scraper.py      # TikTok comments
│   ├── twitter_scraper.py     # Twitter/X API v2
│   ├── facebook_scraper.py    # Facebook (Selenium)
│   ├── optimization.py        # Cache, batch processor, perf monitor
│   ├── advanced_optimization.py  # Parallel processing, memory optimization
│   └── logger.py              # Structured logging
├── models/
│   └── lid.176.ftz            # FastText language identification model
├── data/
│   └── darija_dataset_merged.csv  # Final cleaned dataset (130,851 rows)
├── main.py                    # Entry point (9 modes)
├── install.py                 # Auto-installer
├── quickstart.py              # Quick start guide
├── examples.py                # Usage examples
└── tests.py                   # Unit tests
```

## Installation

```bash
git clone https://github.com/IlyasFardaouixx/darija-dataset-builder.git
cd darija-dataset-builder

pip install -r requirements.txt
# or
python install.py
```

### API Keys (optional, for scraping)

Copy `.env.example` to `.env` and fill in your keys:

```bash
cp .env.example .env
```

| Key | Source | Usage |
|-----|--------|-------|
| `YOUTUBE_API_KEY` | [Google Cloud Console](https://console.cloud.google.com/) | YouTube comments scraping |
| `TWITTER_BEARER_TOKEN` | [Twitter Developer Portal](https://developer.twitter.com/) | Tweet collection |

## Usage

### Interactive Menu

```bash
python main.py
```

Modes available:
1. YouTube scraper
2. Hespress scraper
3. TikTok scraper
4. Twitter/X scraper
5. Facebook scraper
6. Merge all sources
7. Dashboard (collection status)
8. Full pipeline (all scrapers + merge)
9. Synthetic generator only

### Quick Start (no API keys needed)

```bash
python quickstart.py
```

Generates 5,000 synthetic Darija comments using the built-in generator.

### Python API

```python
from src.pipeline import DarijaDatasetPipeline
from src.darija_dataset_generator import generate_dataset_list

# Generate synthetic Darija comments
comments = generate_dataset_list(target_size=10000)

# Process through the pipeline
pipeline = DarijaDatasetPipeline(use_scraper=False)
pipeline.process_comments_batch(comments)
pipeline.save_dataset()
pipeline.print_statistics()
```

### Language Detection

```python
from src.language_detector import LanguageDetector

detector = LanguageDetector()

detector.is_darija("واش كاين شي حاجة جديدة")  # True (Arabic script)
detector.is_darija("wach labas 3lik a sahbi")   # True (Latin Darija)
detector.is_darija("This is English text")       # False
```

## Dataset

The final dataset contains **130,851 unique Darija comments**:

| Metric | Value |
|--------|-------|
| Total comments | 130,851 |
| Arabic script Darija | 130,814 |
| Latin script Darija | 37 |
| Emojis | 0 |
| Foreign language | 0 |
| Sources | YouTube + Synthetic |

### Quality Pipeline

1. **Collection**: 419,826 raw YouTube comments + 50,000 synthetic
2. **Cleaning**: HTML, URLs, emojis stripped
3. **Language filtering**: FastText + Darija word bank heuristics
4. **Foreign rejection**: English, Turkish, Polish, Indonesian, etc.
5. **Deduplication**: MD5 hash-based across all sources

### Darija Word Bank

The detection system uses a hand-crafted word bank covering:
- **Arabic script**: greetings, grammar markers, verbs, nouns, adjectives, expressions
- **Latin script (Arabizi)**: 200+ common Darija words + number phonetics (3, 7, 9, 5, 8, 2)
- **18 themes**: Sport, Musique, Cuisine, Politique, Religion, Humour, Amour, Famille, Education, Technologie, Meteo, Transport, Sante, Economie, Culture, Voyage, Travail, Societe

## Tests

```bash
python tests.py
```

## Performance

| Operation | Speed |
|-----------|-------|
| Text cleaning | ~1,000 ops/sec |
| Language detection | ~500 ops/sec (cached: ~5,000) |
| Full pipeline | ~200 comments/sec |

## License

MIT
