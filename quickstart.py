#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Quick Start - Démarrage rapide du projet Darija Dataset Builder.

Ce script montre comment utiliser le projet en 2 minutes.
Génère un dataset de commentaires Darija prêt à l'emploi.
"""

import sys
import os
import io
import time
from pathlib import Path

# Forcer l'encodage UTF-8 pour la console Windows (arabe)
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

# Ajouter src au chemin
sys.path.insert(0, str(Path(__file__).parent))

from src.pipeline import DarijaDatasetPipeline
from src.darija_dataset_generator import generate_dataset_list, get_all_base_comments
from src.logger import setup_logger

logger = setup_logger(__name__)


def quickstart_example():
    """Exemple de démarrage rapide — génère 5,000 commentaires Darija."""
    
    print("\n" + "="*70)
    print(" "*15 + "🇲🇦 DARIJA DATASET - QUICK START 🇲🇦")
    print("="*70 + "\n")
    
    start = time.time()
    
    # 1. Initialiser le pipeline
    print("1️⃣  Initialisation du pipeline...\n")
    pipeline = DarijaDatasetPipeline(use_scraper=False)
    logger.info("Pipeline prêt!")
    
    # 2. Générer un dataset massif
    base_count = len(get_all_base_comments())
    target = 5000
    print(f"2️⃣  Génération de {target:,} commentaires Darija...")
    print(f"    📚 Banque de départ: {base_count} commentaires authentiques\n")
    
    comments = generate_dataset_list(target_size=target)
    logger.info(f"✓ {len(comments)} commentaires générés")
    
    # 3. Traiter les commentaires (nettoyage + détection Darija)
    print("3️⃣  Traitement: nettoyage + détection Darija (FastText + heuristiques)...\n")
    
    darija_count = pipeline.process_comments_batch(comments)
    logger.info(f"✓ {darija_count} commentaires Darija détectés")
    
    # 4. Sauvegarder le dataset
    print("4️⃣  Sauvegarde du dataset...\n")
    
    output_file = pipeline.save_dataset()
    logger.info(f"✓ Dataset sauvegardé: {output_file}")
    
    # 5. Afficher les statistiques
    print("5️⃣  Statistiques du dataset:\n")
    pipeline.print_statistics()
    
    elapsed = time.time() - start
    
    print("="*70)
    print(f"✅ Quick Start terminé en {elapsed:.1f}s!")
    print(f"📁 Dataset: {output_file}")
    print(f"📊 {darija_count:,} commentaires Darija dans le dataset")
    print("\nProchaines étapes:")
    print("  - Lancez main.py pour générer jusqu'à 100,000 commentaires")
    print("  - Consultez examples.py pour des exemples avancés")
    print("  - Modifiez config/config.py pour personnaliser les seuils")
    print("="*70 + "\n")


def advanced_usage():
    """Exemple d'utilisation avancée avec détails de détection."""
    
    print("\n" + "="*70)
    print(" "*10 + "🔧 DARIJA DATASET - UTILISATION AVANCÉE")
    print("="*70 + "\n")
    
    from config import LANGUAGE_CONFIG, CLEANING_CONFIG
    from src.cleaner import DataCleaner
    from src.language_detector import LanguageDetector
    from src.darija_wordbank import score_darija, has_arabic_script
    
    cleaner = DataCleaner()
    detector = LanguageDetector()
    
    # Textes de test variés
    test_texts = [
        ("Darija arabe", "واح البدر يا سيدي! شحال ديال الجمالة 😊"),
        ("Darija latine", "wach nta mn casa? labas 3lik a sahbi"),
        ("Code-switch", "franchement هادشي واعر بزاف c'est magnifique"),
        ("Arabe standard", "بسم الله الرحمن الرحيم نحن نتقدم بطلب"),
        ("Français pur", "Bonjour comment allez-vous aujourd'hui?"),
        ("Anglais pur", "This is a great post thank you for sharing"),
        ("Darija courte", "مليح بزاف"),
        ("Football Darija", "أسود الأطلس بانو واعرين فكأس العالم 🇲🇦"),
    ]
    
    print("Analyse détaillée de la détection Darija:\n")
    print(f"{'Type':<18} {'Darija?':<8} {'FT Label':<14} {'FT Conf':<9} {'Heur.':<6} {'Texte'}")
    print("-" * 100)
    
    for label, text in test_texts:
        cleaned = cleaner.clean(text)
        details = detector.get_darija_details(cleaned)
        
        darija_mark = "✅" if details["is_darija"] else "❌"
        score_str = f"{details['darija_word_count']}"
        
        print(f"{label:<18} {darija_mark:<8} {details['fasttext_label']:<14} "
              f"{details['fasttext_confidence']:<9.4f} {score_str:<6} {cleaned[:40]}")
    
    print("\n" + "="*70 + "\n")


def main():
    """Menu principal."""
    
    print("\n" + "="*70)
    print(" "*15 + "🇲🇦 DARIJA DATASET BUILDER 🇲🇦")
    print("="*70)
    print("\nChoisissez une option:")
    print("  1. 🚀 Quick Start — Générer 5,000 commentaires Darija")
    print("  2. 🔧 Utilisation Avancée — Détails de détection")
    print("  3. 📊 Dataset Maximum — Lancer main.py (jusqu'à 100K)")
    print("\n")
    
    choice = input("Votre choix (1-3) [1]: ").strip() or "1"
    
    if choice == "1":
        quickstart_example()
    elif choice == "2":
        advanced_usage()
    elif choice == "3":
        print("\n▶ Lancement de main.py...\n")
        try:
            from main import main as main_func
            main_func()
        except Exception as e:
            print(f"❌ Erreur: {e}")
    else:
        print("❌ Choix invalide, lancement du Quick Start...")
        quickstart_example()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⛔ Opération annulée par l'utilisateur")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Erreur: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
