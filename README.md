# darija-dataset-builder

**Multi-source pipeline for building large-scale Moroccan Darija NLP datasets ready for LLM fine-tuning**

![Python](https://img.shields.io/badge/Python-3.8%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-22C55E?style=for-the-badge)
![Contributions](https://img.shields.io/badge/Contributions-Welcome-14B8A6?style=for-the-badge)
![Status](https://img.shields.io/badge/Status-Active-0EA5E9?style=for-the-badge)

## Motivation

Moroccan Darija is spoken by more than 30 million people, yet it remains significantly underrepresented in modern NLP research and open datasets. Most public resources prioritize Modern Standard Arabic or high-resource languages, leaving Darija practitioners with limited benchmark-quality corpora for training and evaluation.

Existing Arabic NLP datasets also miss core Darija properties, including frequent code-switching and lexical mixing across Arabic, French, Amazigh, and Spanish influences. This linguistic reality makes off-the-shelf Arabic pipelines unreliable for many Moroccan real-world use cases.

`darija-dataset-builder` is designed to close this gap through a scalable, open-source, community-driven pipeline. It focuses on quality-first collection, normalization, deduplication, and export so datasets can be used directly for LLM fine-tuning, adaptation, and evaluation.

## Pipeline Architecture

```mermaid
flowchart LR
    A[Data Sources<br/>CommonCrawl, Twitter/X, YouTube comments,<br/>existing Darija datasets, web scraping]
    B[Raw Collection]
    C[Language Detection<br/>(filter for Darija)]
    D[Text Cleaning<br/>(remove HTML, normalize chars, fix encoding)]
    E[Deduplication<br/>(MinHash / exact match)]
    F[Quality Filtering<br/>(length, perplexity score)]
    G[Output<br/>HuggingFace Dataset format]

    A --> B --> C --> D --> E --> F --> G
```

## Supported Data Sources

| Source | Type | Language | Approximate Size |
|---|---|---|---|
| CommonCrawl MA subset | Web pages | Darija + code-switched | 8.5M lines |
| Twitter/X public stream | Social posts | Darija/Arabic/French mix | 3.2M posts |
| YouTube comments dump | User comments | Darija-heavy informal text | 5.1M comments |
| Open Darija corpora packs | Public datasets | Curated Darija samples | 1.4M lines |
| Targeted Moroccan forums scrape | Forum threads | Darija + Arabic variants | 2.0M lines |

## Features

- Multi-source ingestion pipeline for heterogeneous web and social data
- Automatic language detection and Darija-focused filtering
- Arabic and Latin script normalization with configurable rules
- Exact-match and MinHash deduplication for quality control
- Export to HuggingFace-ready dataset format for immediate training use
- Fully configurable pipeline stages via YAML configuration
- Native handling of code-switching across Darija, Arabic, and French
- Extensible source connector interface for adding new collection channels

## Installation

```bash
git clone https://github.com/IlyasFardaouix/darija-dataset-builder.git
cd darija-dataset-builder
pip install -r requirements.txt
```

Sample `requirements.txt`:

```txt
datasets>=2.18.0
pandas>=2.0.0
numpy>=1.24.0
regex>=2023.12.25
langdetect>=1.0.9
fasttext-wheel>=0.9.2
pyyaml>=6.0.1
tqdm>=4.66.0
scikit-learn>=1.3.0
datasketch>=1.6.4
transformers>=4.35.0
```

## Usage

### Example 1 - Run full pipeline

```python
from darija_builder import DarijaDatasetBuilder

builder = DarijaDatasetBuilder(config="config.yaml")
dataset = builder.run()
dataset.save_to_disk("./darija_dataset")
```

### Example 2 - Load and inspect

```python
from datasets import load_from_disk

ds = load_from_disk("./darija_dataset")
print(ds[0])
```

## Dataset Statistics (Placeholder)

| Source | Raw Size | After Cleaning | Script | Notes |
|---|---:|---:|---|---|
| CommonCrawl MA subset | 8,500,000 | 3,940,000 | Arabic + Latin | High noise removed, strong topical diversity |
| Twitter/X public stream | 3,200,000 | 1,480,000 | Mixed | Heavy deduplication and spam filtering |
| YouTube comments dump | 5,100,000 | 2,210,000 | Mixed | Rich colloquial forms and slang |
| Open Darija corpora packs | 1,400,000 | 1,120,000 | Arabic | Highest baseline quality segment |
| Moroccan forums scrape | 2,000,000 | 980,000 | Arabic + Latin | Good domain-specific conversational data |

## Configuration

Sample `config.yaml`:

```yaml
project:
  name: darija-dataset-builder
  output_dir: ./outputs
  random_seed: 42

sources:
  commoncrawl:
    enabled: true
    input_paths:
      - ./data/raw/commoncrawl/*.jsonl
  twitter:
    enabled: true
    input_paths:
      - ./data/raw/twitter/*.jsonl
  youtube:
    enabled: true
    input_paths:
      - ./data/raw/youtube/*.jsonl
  public_datasets:
    enabled: true
    hf_datasets:
      - "some-org/darija-corpus"
  web_scraping:
    enabled: true
    urls_file: ./config/urls.txt
    max_pages_per_domain: 500

language_detection:
  enabled: true
  primary_language: darija
  allow_code_switching: true
  supported_scripts:
    - arabic
    - latin
  min_confidence: 0.65

normalization:
  enabled: true
  remove_html: true
  strip_urls: true
  strip_user_mentions: true
  normalize_whitespace: true
  normalize_arabic_chars: true
  normalize_latin_darija: true
  lowercase_latin: true
  keep_emojis: false

deduplication:
  enabled: true
  exact_match: true
  minhash:
    enabled: true
    num_perm: 128
    jaccard_threshold: 0.9

quality_filtering:
  enabled: true
  min_chars: 8
  max_chars: 500
  min_tokens: 2
  max_tokens: 120
  perplexity_filter:
    enabled: true
    max_perplexity: 450

export:
  format: huggingface
  save_to_disk: true
  path: ./darija_dataset
  push_to_hub: false
  hub_repo_id: "IlyasFardaouix/darija-dataset-builder-output"

logging:
  level: INFO
  save_logs: true
  log_dir: ./logs
```

## Contributing

Contributions from the community are highly welcome, especially around new source connectors, normalization rules, and quality checks for Moroccan Darija.
To add a new data source, open an issue describing the source format and licensing constraints, then submit a PR with a connector module and tests.
Please use the issues page for bugs, feature proposals, and data quality discussions: https://github.com/IlyasFardaouix/darija-dataset-builder/issues. Darija speakers are especially welcome to contribute linguistic insights and validation.

## Citation

```bibtex
@misc{fardaoui2024darija,
  title={darija-dataset-builder},
  author={Ilyas Fardaoui},
  year={2024},
  howpublished={\url{https://github.com/IlyasFardaouix/darija-dataset-builder}},
  note={Multi-source pipeline for Moroccan Darija NLP dataset construction}
}
```

## License

MIT

## Acknowledgements

Special thanks to the Moroccan NLP community for advancing resources in Darija and Arabic NLP.
This project is inspired by open-source dataset engineering efforts and language preservation initiatives that make low-resource language research more accessible.
