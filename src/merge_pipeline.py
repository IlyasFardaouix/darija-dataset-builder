# -*- coding: utf-8 -*-
"""
Pipeline Central de Fusion — Fusionne TOUTES les sources en un seul dataset Darija.

Architecture multi-pipeline:
1. Chaque scraper collecte les données brutes en JSONL
2. Ce pipeline central lit TOUS les JSONL
3. Applique nettoyage + filtrage Darija + déduplication à grande échelle
4. Fusionne dans un CSV unique compatible Hugging Face

Sources supportées:
- YouTube (~400k commentaires)
- Hespress (~300k commentaires)
- TikTok (~250k commentaires)  
- Facebook (~150k commentaires)
- Twitter/X (~100k commentaires)
- Générateur synthétique (commentaires de base)

Objectif: ~1 million de commentaires Darija uniques.
"""

import json
import hashlib
import time
import pandas as pd
from pathlib import Path
from typing import Dict, Generator, Set, List, Optional
from src.cleaner import DataCleaner
from src.language_detector import LanguageDetector
from config import CSV_CONFIG, DATA_DIR
from src.logger import setup_logger

logger = setup_logger(__name__)

RAW_DATA_DIR = DATA_DIR / "raw"
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Mapping source → fichier JSONL brut
SOURCE_FILES = {
    "youtube":   RAW_DATA_DIR / "youtube_comments.jsonl",
    "hespress":  RAW_DATA_DIR / "hespress_comments.jsonl",
    "tiktok":    RAW_DATA_DIR / "tiktok_comments.jsonl",
    "twitter":   RAW_DATA_DIR / "twitter_comments.jsonl",
    "facebook":  RAW_DATA_DIR / "facebook_comments.jsonl",
}

# Fichier CSV final fusionné
MERGED_CSV = DATA_DIR / "darija_dataset_merged.csv"
MERGE_PROGRESS_FILE = RAW_DATA_DIR / "merge_progress.json"


class MergePipeline:
    """
    Pipeline central de fusion et filtrage Darija.
    
    Lit les données brutes JSONL de chaque source,
    applique le nettoyage / filtrage / déduplication,
    et produit un CSV final unique.
    """
    
    def __init__(self):
        self.cleaner = DataCleaner()
        self.language_detector = LanguageDetector()
        self.stats = {
            "total_raw": 0,
            "total_cleaned": 0,
            "total_darija": 0,
            "total_duplicates": 0,
            "per_source": {},
        }
        self._seen_hashes: Set[str] = set()
    
    @staticmethod
    def _text_hash(text: str) -> str:
        """Hash MD5 pour déduplication rapide."""
        normalized = text.strip().lower()
        return hashlib.md5(normalized.encode('utf-8')).hexdigest()
    
    def _read_jsonl(self, filepath: Path) -> Generator[Dict, None, None]:
        """Lit un fichier JSONL ligne par ligne."""
        if not filepath.exists():
            return
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        continue
    
    def _count_jsonl(self, filepath: Path) -> int:
        """Compte les lignes d'un fichier JSONL."""
        if not filepath.exists():
            return 0
        count = 0
        with open(filepath, 'r', encoding='utf-8') as f:
            for _ in f:
                count += 1
        return count
    
    def get_source_counts(self) -> Dict[str, int]:
        """Retourne le nombre de commentaires bruts par source."""
        counts = {}
        for source, filepath in SOURCE_FILES.items():
            counts[source] = self._count_jsonl(filepath)
        return counts
    
    def process_source(self, source_name: str, filepath: Path) -> Generator[Dict, None, None]:
        """
        Traite les commentaires bruts d'une source:
        nettoyage → validation → détection Darija → déduplication.
        
        Args:
            source_name: Nom de la source (youtube, hespress, etc.)
            filepath: Chemin du fichier JSONL brut
            
        Yields:
            Dict {"text": ..., "url": ..., "source": ...}
        """
        source_raw = 0
        source_darija = 0
        source_dupes = 0
        
        for record in self._read_jsonl(filepath):
            source_raw += 1
            self.stats["total_raw"] += 1
            
            text = record.get("text", "").strip()
            url = record.get("url", "")
            
            if not text:
                continue
            
            # Nettoyage
            cleaned = self.cleaner.clean(text)
            if not cleaned or not self.cleaner.is_valid_length(cleaned):
                continue
            
            self.stats["total_cleaned"] += 1
            
            # Déduplication
            h = self._text_hash(cleaned)
            if h in self._seen_hashes:
                source_dupes += 1
                self.stats["total_duplicates"] += 1
                continue
            
            # Détection Darija
            if self.language_detector.is_darija(cleaned):
                self._seen_hashes.add(h)
                source_darija += 1
                self.stats["total_darija"] += 1
                
                yield {
                    "text": cleaned,
                    "url": url,
                }
        
        self.stats["per_source"][source_name] = {
            "raw": source_raw,
            "darija": source_darija,
            "duplicates": source_dupes,
        }
        
        logger.info(
            f"{source_name}: {source_raw:,} bruts → "
            f"{source_darija:,} Darija ({source_dupes:,} doublons)"
        )
    
    def merge_all(self, include_generated: bool = True,
                  generated_size: int = 50000,
                  output_file: str = None) -> str:
        """
        Fusionne TOUTES les sources en un seul CSV final.
        
        Args:
            include_generated: Inclure les commentaires du générateur synthétique
            generated_size: Taille du dataset généré (si include_generated)
            output_file: Chemin du CSV final (défaut: data/darija_dataset_merged.csv)
            
        Returns:
            Chemin du fichier CSV final
        """
        output_file = Path(output_file or MERGED_CSV)
        start_time = time.time()
        
        print(f"\n{'='*70}")
        print(f"  🔄 PIPELINE CENTRAL — Fusion multi-source Darija")
        print(f"{'='*70}\n")
        
        # Afficher les compteurs bruts
        source_counts = self.get_source_counts()
        print("📊 Données brutes disponibles:")
        total_raw = 0
        for source, count in source_counts.items():
            status = "✅" if count > 0 else "⬜"
            print(f"   {status} {source:12s}: {count:>10,} commentaires")
            total_raw += count
        print(f"   {'─'*35}")
        print(f"   📦 Total brut:    {total_raw:>10,}")
        if include_generated:
            print(f"   🤖 + Généré:      {generated_size:>10,}")
        print()
        
        # Traiter chaque source
        all_records = []
        
        print("🔧 Traitement source par source (nettoyage + Darija + dédupe):\n")
        
        for source_name, filepath in SOURCE_FILES.items():
            if not filepath.exists() or self._count_jsonl(filepath) == 0:
                print(f"   ⬜ {source_name:12s}: pas de données")
                continue
            
            print(f"   ⏳ {source_name:12s}: traitement...", end=" ", flush=True)
            source_records = list(self.process_source(source_name, filepath))
            all_records.extend(source_records)
            src_stats = self.stats["per_source"].get(source_name, {})
            print(f"→ {src_stats.get('darija', 0):,} Darija "
                  f"(de {src_stats.get('raw', 0):,} bruts)")
        
        # Ajouter les commentaires du générateur synthétique
        if include_generated:
            print(f"\n   ⏳ {'generated':12s}: génération de {generated_size:,}...", end=" ", flush=True)
            from src.darija_dataset_generator import generate_massive_dataset
            gen_count = 0
            gen_dupes = 0
            for record in generate_massive_dataset(generated_size):
                text = record.get("text", "").strip()
                h = self._text_hash(text)
                if h not in self._seen_hashes:
                    self._seen_hashes.add(h)
                    all_records.append(record)
                    gen_count += 1
                else:
                    gen_dupes += 1
            
            self.stats["per_source"]["generated"] = {
                "raw": generated_size,
                "darija": gen_count,
                "duplicates": gen_dupes,
            }
            self.stats["total_darija"] += gen_count
            print(f"→ {gen_count:,} uniques")
        
        # Créer le DataFrame final
        print(f"\n📝 Construction du DataFrame final...")
        df = pd.DataFrame(all_records, columns=["text", "url"])
        
        # Dernière passe de déduplication (au cas où)
        initial = len(df)
        df = df.drop_duplicates(subset=["text"], keep="first")
        final_dupes = initial - len(df)
        if final_dupes > 0:
            print(f"   Supprimé {final_dupes:,} doublons restants")
        
        # Sauvegarder
        output_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_file, index=False, encoding="utf-8-sig")
        
        elapsed = time.time() - start_time
        
        # Statistiques finales
        print(f"\n{'='*70}")
        print(f"  ✅ FUSION TERMINÉE — Dataset Darija prêt!")
        print(f"{'='*70}")
        print(f"  📊 Commentaires Darija uniques: {len(df):,}")
        print(f"  📊 Bruts traités:               {self.stats['total_raw']:,}")
        print(f"  📊 Doublons supprimés:           {self.stats['total_duplicates'] + final_dupes:,}")
        print(f"  ⏱️  Temps de traitement:          {elapsed:.1f}s")
        print(f"  📁 Fichier CSV:                 {output_file}")
        print(f"  📐 Taille:                      {output_file.stat().st_size / (1024*1024):.1f} MB")
        print(f"\n  📊 Détail par source:")
        for source, src_stats in self.stats["per_source"].items():
            pct = src_stats['darija'] / max(len(df), 1) * 100
            print(f"     {source:12s}: {src_stats['darija']:>8,} ({pct:>5.1f}%)")
        print(f"{'='*70}\n")
        
        # Sauvegarder les stats
        stats_file = RAW_DATA_DIR / "merge_stats.json"
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump({
                "total_darija": len(df),
                "total_raw": self.stats['total_raw'],
                "total_duplicates": self.stats['total_duplicates'] + final_dupes,
                "per_source": self.stats["per_source"],
                "elapsed_seconds": round(elapsed, 2),
                "output_file": str(output_file),
            }, f, ensure_ascii=False, indent=2)
        
        return str(output_file)
    
    def print_dashboard(self):
        """Affiche un tableau de bord complet des données collectées."""
        source_counts = self.get_source_counts()
        
        print(f"\n{'='*70}")
        print(f"  📊 TABLEAU DE BORD — Darija Dataset Builder")
        print(f"{'='*70}\n")
        
        total = 0
        for source, count in source_counts.items():
            bar_len = min(count // 1000, 40)
            bar = "█" * bar_len
            status = "✅" if count > 0 else "⬜"
            print(f"  {status} {source:12s} │ {count:>10,} │ {bar}")
            total += count
        
        print(f"  {'─'*50}")
        print(f"  📦 Total brut      │ {total:>10,} │")
        
        # Vérifier si le CSV fusionné existe
        if MERGED_CSV.exists():
            try:
                df = pd.read_csv(MERGED_CSV, encoding='utf-8-sig', nrows=0)
                # Compter les lignes sans tout charger
                with open(MERGED_CSV, 'r', encoding='utf-8-sig') as f:
                    merged_count = sum(1 for _ in f) - 1  # -1 pour le header
                print(f"  ✅ CSV fusionné     │ {merged_count:>10,} │ Darija uniques")
                size_mb = MERGED_CSV.stat().st_size / (1024*1024)
                print(f"  📐 Taille fichier   │ {size_mb:>9.1f}M │")
            except Exception:
                pass
        else:
            print(f"  ⬜ CSV fusionné     │ pas encore │ Lancez la fusion!")
        
        print(f"{'='*70}\n")
