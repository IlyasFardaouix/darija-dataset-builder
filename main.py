# -*- coding: utf-8 -*-
"""
🇲🇦 DARIJA DATASET BUILDER — Architecture Multi-Pipeline

Collecte massive de commentaires Darija depuis 5 sources:
  - YouTube  (~400k) via API Data v3
  - Hespress (~300k) via scraping web
  - TikTok   (~250k) via scraping web
  - Facebook (~150k) via Selenium
  - Twitter/X (~100k) via API v2

Pipeline central: nettoyage → filtrage Darija → déduplication → CSV Hugging Face
Objectif: ~1 million de commentaires Darija uniques.
"""

import sys
import os
import io
import time

# Forcer l'encodage UTF-8 pour la console Windows (arabe)
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

from src.logger import setup_logger

logger = setup_logger(__name__)


def main():
    """Menu principal — Darija Dataset Builder multi-source."""
    
    print("\n" + "="*70)
    print("  🇲🇦 DARIJA DATASET BUILDER — Multi-Pipeline (~1M commentaires)")
    print("="*70 + "\n")
    
    print("  Choisissez un mode:\n")
    print("  ── COLLECTE (scrapers individuels) ──")
    print("  1. 🎬 YouTube     — Scraper les commentaires YouTube marocains")
    print("  2. 📰 Hespress    — Scraper les commentaires Hespress")
    print("  3. 🎵 TikTok      — Scraper les commentaires TikTok marocains")
    print("  4. 🐦 Twitter/X   — Collecter les tweets Darija")
    print("  5. 🌐 Facebook    — Scraper Facebook (Selenium)")
    print()
    print("  ── TRAITEMENT ──")
    print("  6. 🔄 FUSION      — Fusionner TOUTES les sources en 1 CSV final")
    print("  7. 📊 Dashboard   — Voir l'état de la collecte")
    print()
    print("  ── QUICK ──")
    print("  8. 🚀 TOUT LANCER — Collecter TOUTES les sources + fusionner")
    print("  9. 🤖 Générer     — Dataset synthétique uniquement (hors-ligne)")
    print()
    
    choice = input("  Votre choix (1-9) [8]: ").strip() or "8"
    
    actions = {
        "1": mode_youtube,
        "2": mode_hespress,
        "3": mode_tiktok,
        "4": mode_twitter,
        "5": mode_facebook,
        "6": mode_merge,
        "7": mode_dashboard,
        "8": mode_all,
        "9": mode_generate,
    }
    
    action = actions.get(choice, mode_all)
    action()


# ============================================================================
# MODE 1: YOUTUBE
# ============================================================================

def mode_youtube():
    """Scraper YouTube — commentaires de vidéos marocaines."""
    try:
        from src.youtube_scraper import YouTubeScraper
        scraper = YouTubeScraper()
        scraper.scrape_all()
    except ValueError as e:
        print(f"\n❌ {e}")
    except Exception as e:
        logger.error(f"Erreur YouTube: {e}")
        print(f"\n❌ Erreur YouTube: {e}")


# ============================================================================
# MODE 2: HESPRESS
# ============================================================================

def mode_hespress():
    """Scraper Hespress — commentaires d'articles d'actualités."""
    try:
        from src.hespress_scraper import HespressScraper
        scraper = HespressScraper()
        scraper.scrape_all()
    except Exception as e:
        logger.error(f"Erreur Hespress: {e}")
        print(f"\n❌ Erreur Hespress: {e}")


# ============================================================================
# MODE 3: TIKTOK
# ============================================================================

def mode_tiktok():
    """Scraper TikTok — commentaires de vidéos marocaines."""
    try:
        from src.tiktok_scraper import TikTokScraper
        scraper = TikTokScraper()
        scraper.scrape_all()
    except Exception as e:
        logger.error(f"Erreur TikTok: {e}")
        print(f"\n❌ Erreur TikTok: {e}")


# ============================================================================
# MODE 4: TWITTER
# ============================================================================

def mode_twitter():
    """Scraper Twitter/X — tweets en Darija (Free tier ~1500/mois)."""
    try:
        from src.twitter_scraper import TwitterScraper
        scraper = TwitterScraper()
        scraper.scrape_all()
    except ValueError as e:
        print(f"\n❌ {e}")
    except Exception as e:
        logger.error(f"Erreur Twitter: {e}")
        print(f"\n❌ Erreur Twitter: {e}")


# ============================================================================
# MODE 5: FACEBOOK
# ============================================================================

def mode_facebook():
    """Scraper Facebook — commentaires de publications (Selenium)."""
    from src.pipeline import DarijaDatasetPipeline
    
    print("\n" + "-"*60)
    print("🌐 MODE SCRAPER FACEBOOK")
    print("-"*60 + "\n")
    
    post_urls = [
        # ⚠️ Ajoutez vos URLs Facebook ici
    ]
    
    if not post_urls:
        print("⚠️  Aucune URL configurée!")
        print("   Éditez main.py → mode_facebook() et ajoutez vos URLs.")
        print('   Exemple: "https://www.facebook.com/hesaborima/posts/123456"')
        return
    
    try:
        pipeline = DarijaDatasetPipeline(use_scraper=True)
        pipeline.process_multiple_posts(post_urls)
        
        # Sauvegarder en JSONL brut pour la fusion ultérieure
        import json
        from pathlib import Path
        raw_dir = Path(__file__).parent / "data" / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        fb_file = raw_dir / "facebook_comments.jsonl"
        
        with open(fb_file, 'a', encoding='utf-8') as f:
            for record in pipeline.csv_manager.data:
                f.write(json.dumps(record, ensure_ascii=False) + '\n')
        
        pipeline.save_dataset()
        pipeline.print_statistics()
        pipeline.close()
        
    except Exception as e:
        logger.error(f"Erreur Facebook: {e}")
        print(f"\n❌ Erreur: {e}")
        print("Vérifiez que Chrome et ChromeDriver sont installés.")


# ============================================================================
# MODE 6: FUSION CENTRALE
# ============================================================================

def mode_merge():
    """Fusionne TOUTES les sources collectées en un seul CSV Darija."""
    from src.merge_pipeline import MergePipeline
    
    print("\nTaille du dataset généré (complément synthétique):")
    print("  1. 🟢 Aucun   — Seulement les données scrapées")
    print("  2. 🟡 50k     — Ajouter 50,000 commentaires générés")
    print("  3. 🟠 100k    — Ajouter 100,000 commentaires générés")
    print("  4. 🔴 200k    — Ajouter 200,000 commentaires générés")
    print()
    
    gen_choice = input("  Choix (1-4) [3]: ").strip() or "3"
    gen_sizes = {"1": 0, "2": 50000, "3": 100000, "4": 200000}
    gen_size = gen_sizes.get(gen_choice, 100000)
    
    pipeline = MergePipeline()
    pipeline.merge_all(
        include_generated=(gen_size > 0),
        generated_size=gen_size
    )


# ============================================================================
# MODE 7: DASHBOARD
# ============================================================================

def mode_dashboard():
    """Affiche le tableau de bord de la collecte."""
    from src.merge_pipeline import MergePipeline
    pipeline = MergePipeline()
    pipeline.print_dashboard()


# ============================================================================
# MODE 8: TOUT LANCER
# ============================================================================

def mode_all():
    """Lance TOUS les scrapers puis fusionne — mode automatique complet."""
    from src.merge_pipeline import MergePipeline
    
    print("\n" + "="*70)
    print("  🚀 MODE COMPLET — Collecte multi-source + Fusion")
    print("="*70 + "\n")
    
    start_time = time.time()
    
    # 1. YouTube
    print("━"*70)
    print("  [1/5] 🎬 YouTube")
    print("━"*70)
    try:
        from src.youtube_scraper import YouTubeScraper
        yt = YouTubeScraper()
        yt.scrape_all()
    except Exception as e:
        print(f"  ⚠️  YouTube: {e}")
    
    # 2. Hespress
    print("━"*70)
    print("  [2/5] 📰 Hespress")
    print("━"*70)
    try:
        from src.hespress_scraper import HespressScraper
        hp = HespressScraper()
        hp.scrape_all()
    except Exception as e:
        print(f"  ⚠️  Hespress: {e}")
    
    # 3. TikTok
    print("━"*70)
    print("  [3/5] 🎵 TikTok")
    print("━"*70)
    try:
        from src.tiktok_scraper import TikTokScraper
        tt = TikTokScraper()
        tt.scrape_all()
    except Exception as e:
        print(f"  ⚠️  TikTok: {e}")
    
    # 4. Twitter
    print("━"*70)
    print("  [4/5] 🐦 Twitter/X")
    print("━"*70)
    try:
        from src.twitter_scraper import TwitterScraper
        tw = TwitterScraper()
        tw.scrape_all()
    except Exception as e:
        print(f"  ⚠️  Twitter: {e}")
    
    # 5. Fusion
    print("━"*70)
    print("  [5/5] 🔄 Fusion de toutes les sources")
    print("━"*70)
    pipeline = MergePipeline()
    pipeline.merge_all(include_generated=True, generated_size=100000)
    
    elapsed = time.time() - start_time
    print(f"\n🏁 Collecte complète terminée en {elapsed/60:.1f} minutes!")


# ============================================================================
# MODE 9: GÉNÉRATEUR SYNTHÉTIQUE UNIQUEMENT
# ============================================================================

def mode_generate():
    """Génère un dataset synthétique Darija (hors-ligne, pas besoin d'API)."""
    from src.darija_dataset_generator import generate_massive_dataset, get_all_base_comments
    from src.pipeline import DarijaDatasetPipeline
    
    print("\n" + "-"*60)
    print("🤖 MODE GÉNÉRATEUR SYNTHÉTIQUE")
    print("-"*60 + "\n")
    
    print("Taille du dataset:")
    print("  1. 🟢 1,000    2. 🟡 5,000    3. 🟠 10,000")
    print("  4. 🔴 50,000   5. 🔥 100,000  6. 💥 200,000")
    print()
    
    size_choice = input("  Choix (1-6) [4]: ").strip() or "4"
    sizes = {"1": 1000, "2": 5000, "3": 10000, "4": 50000, "5": 100000, "6": 200000}
    target_size = sizes.get(size_choice, 50000)
    
    base_count = len(get_all_base_comments())
    print(f"\n📚 Banque: {base_count} commentaires authentiques")
    print(f"🚀 Génération de {target_size:,} commentaires...\n")
    
    start_time = time.time()
    pipeline = DarijaDatasetPipeline(use_scraper=False)
    
    comments_generator = generate_massive_dataset(target_size)
    total_darija = pipeline.process_comments_streaming(
        comments_generator, batch_size=2000, save_every=5000
    )
    
    output_file = pipeline.save_dataset()
    elapsed = time.time() - start_time
    
    print(f"\n✅ Généré en {elapsed:.1f}s!")
    print(f"📁 Fichier: {output_file}")
    pipeline.print_statistics()


# ============================================================================
# POINT D'ENTRÉE
# ============================================================================

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⛔ Opération annulée par l'utilisateur")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Erreur fatale: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
