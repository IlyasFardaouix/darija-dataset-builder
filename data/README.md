---
language:
- ar
license: mit
tags:
- darija
- moroccan-arabic
- nlp
- text-classification
- sentiment-analysis
- morocco
- dialect
- arabic-dialect
pretty_name: Moroccan Darija Comments Dataset
size_categories:
- 100K<n<1M
task_categories:
- text-classification
- text-generation
dataset_info:
  features:
  - name: text
    dtype: string
  - name: url
    dtype: string
  splits:
  - name: train
    num_examples: 130851
---

# Moroccan Darija Comments Dataset 🇲🇦

A large-scale dataset of **130,851 Darija (Moroccan Arabic)** comments collected from YouTube and synthetic generation, designed for NLP tasks on the Moroccan dialect.

## Dataset Description

### Overview

This dataset contains authentic and augmented Darija comments covering diverse topics including sports, music, cuisine, news, culture, humor, and daily life in Morocco. It is built for training and evaluating NLP models on Moroccan Arabic — a low-resource dialect that is underrepresented in existing Arabic NLP datasets.

### Sources

| Source | Comments | Description |
|--------|----------|-------------|
| YouTube | ~81,000 | Real comments from 400k+ scraped Moroccan YouTube comments |
| Synthetic Generator | ~50,000 | Augmented Darija comments across 18 themes |
| **Total** | **130,851** | Unique, deduplicated, clean Darija comments |

### Data Fields

- **text** (`string`): The Darija comment text (cleaned, validated, emoji-free)
- **url** (`string`): Source URL (YouTube video link for scraped data)

### Data Processing Pipeline

1. **Collection**: YouTube Data API v3 — comments from 46+ videos across 20 Moroccan channels and 29 Darija search queries (419,826 raw comments)
2. **Cleaning**: HTML removal, full emoji stripping, whitespace cleanup
3. **Language Detection**: Hybrid Darija detection using FastText + Darija word-bank heuristics (supports both Arabic script and Latin Darija)
4. **Foreign Language Rejection**: English, Turkish, Polish, Indonesian, and 8 other foreign languages explicitly rejected
5. **Deduplication**: MD5 hash-based deduplication across all sources
6. **Augmentation**: 564 base Darija comments expanded via 7 variation strategies (word substitution, punctuation, etc.)

### Quality Guarantees

- **Zero emojis** — all Unicode emojis and symbols stripped
- **Zero foreign languages** — English, Turkish, Polish, Indonesian, etc. all removed
- **Latin Darija preserved** — Darija written in Latin script with number phonetics (3=ع, 7=ح, 9=ق) is kept
- **Arabic Darija preserved** — Standard Darija in Arabic script fully retained

### Statistics

- **Total comments**: 130,851
- **Arabic Darija**: 130,814 comments
- **Latin Darija**: 37 comments
- **Encoding**: UTF-8 with BOM (utf-8-sig)

### Themes Covered (18 categories)

Sport, Musique, Cuisine, Politique, Religion, Humour, Amour, Famille, Éducation, Technologie, Météo, Transport, Santé, Économie, Culture, Voyage, Travail, Société

### Languages

- **Primary**: Darija (Moroccan Arabic) — اللهجة المغربية
- **Script**: Arabic script + Latin Darija (with number phonetics: 3, 7, 9)
- **Mixed content**: Some comments include French/Arabic code-switching, which is natural in Darija

## Usage

```python
from datasets import load_dataset

dataset = load_dataset("IlyasFardaouixx/moroccan-darija-dataset")

# Access the data
for example in dataset["train"]:
    print(example["text"])
```

## Use Cases

- **Sentiment Analysis** on Moroccan social media
- **Text Classification** for Darija content moderation
- **Language Model fine-tuning** for Moroccan Arabic
- **Dialect Detection** (Darija vs MSA vs other Arabic dialects)
- **Machine Translation** (Darija ↔ French / English / MSA)

## Citation

```bibtex
@dataset{moroccan_darija_dataset_2026,
  title={Moroccan Darija Comments Dataset},
  author={Ilyas Fardaoui},
  year={2026},
  url={https://huggingface.co/datasets/IlyasFardaouixx/moroccan-darija-dataset},
  note={66,280 Darija comments from YouTube and synthetic augmentation}
}
```

## License

This dataset is released under the [MIT License](https://opensource.org/licenses/MIT).
