"""
Script d'exemple avancé montrant toutes les fonctionnalités
avec optimisations de performance.
"""

from src.pipeline import DarijaDatasetPipeline
from src.logger import setup_logger
from src.optimization import performance_monitor, cache_manager
import time

logger = setup_logger(__name__)


def example_basic_usage():
    """Exemple basique: traiter des commentaires directement."""
    print("\n" + "="*60)
    print("EXEMPLE 1: Traitement Basique de Commentaires")
    print("="*60 + "\n")
    
    pipeline = DarijaDatasetPipeline(use_scraper=False)
    
    # Commentaires d'exemple
    comments = [
        {
            "text": "واح البدر يا سيدي! شحال ديال الجمالة في هذا الوقت",
            "url": "https://www.facebook.com/post/123"
        },
        {
            "text": "مليح البزاف يا لخوي! غادي نشوفك قريب إن شاء الله",
            "url": "https://www.facebook.com/post/456"
        },
        {
            "text": "نحن ندعم اللغة العربية الفصحى والدارجة المغربية",
            "url": "https://www.facebook.com/post/789"
        },
        {
            "text": "This is an English comment that will be filtered",
            "url": "https://www.facebook.com/post/123"
        },
        {
            "text": "السلام عليكم ورحمة الله وبركاته، كيفاش تاع الحوال",
            "url": "https://www.facebook.com/post/1011"
        },
    ]
    
    logger.info(f"Traitement de {len(comments)} commentaires...")
    darija_count = pipeline.process_comments_batch(comments)
    
    pipeline.save_dataset()
    pipeline.print_statistics()
    
    return pipeline


def example_with_batching():
    """Exemple avec traitement par lots optimisé."""
    print("\n" + "="*60)
    print("EXEMPLE 2: Traitement Optimisé par Lots")
    print("="*60 + "\n")
    
    from src.optimization import OptimizedBatchProcessor
    from src.cleaner import DataCleaner
    
    cleaner = DataCleaner()
    
    # Générer des commentaires de test
    sample_texts = [
        "واح البدر يا سيدي! شحال ديال الجمالة",
        "مليح البزاف يا لخوي! غادي نشوفك قريب",
        "This is English text",
        "نحن ندعم اللغة العربية الفصحى",
        "Bonjour mon ami, ça va?",
        "السلام عليكم ورحمة الله وبركاته",
    ] * 100  # Répéter pour avoir 600 textes
    
    print(f"Nettoyage de {len(sample_texts)} textes par lots...\n")
    
    start_time = time.time()
    cleaned_texts = OptimizedBatchProcessor.process_with_batching(
        cleaner.clean_batch,
        sample_texts,
        batch_size=100
    )
    elapsed = time.time() - start_time
    
    print(f"✓ {len(cleaned_texts)} textes nettoyés en {elapsed:.2f}s")
    print(f"✓ Performance: {len(sample_texts)/elapsed:.0f} textes/seconde\n")


def example_language_detection():
    """Exemple de détection de langue avec cache."""
    print("\n" + "="*60)
    print("EXEMPLE 3: Détection de Langue avec Cache")
    print("="*60 + "\n")
    
    from src.language_detector import LanguageDetector
    
    detector = LanguageDetector()
    
    test_texts = [
        "واح البدر يا سيدي! شحال ديال الجمالة",
        "مليح البزاف يا لخوي",
        "This is an English text",
        "Bonjour mon ami",
        # Répéter pour démontrer le cache
        "واح البدر يا سيدي! شحال ديال الجمالة",
        "مليح البزاف يا لخوي",
    ]
    
    print("Détection de langue:")
    for text in test_texts:
        lang, confidence = detector.detect_language(text)
        is_darija = detector.is_darija(text)
        print(f"  {text[:40]:<42} → {lang} (conf: {confidence:.2f}, Darija: {is_darija})")
    
    print("\n" + "Cache Statistics:")
    cache_stats = cache_manager.get_stats()
    print(f"  Size: {cache_stats['size']}")
    print(f"  Hits: {cache_stats['hits']}")
    print(f"  Misses: {cache_stats['misses']}")
    print(f"  Hit Rate: {cache_stats['hit_rate']:.2f}%\n")


def example_csv_operations():
    """Exemple d'opérations CSV."""
    print("\n" + "="*60)
    print("EXEMPLE 4: Opérations CSV")
    print("="*60 + "\n")
    
    from src.csv_manager import CSVManager
    
    csv_mgr = CSVManager("data/example_output.csv")
    
    # Ajouter des enregistrements
    records = [
        {"text": "واح البدر يا سيدي!", "url": "https://facebook.com/1"},
        {"text": "مليح البزاف", "url": "https://facebook.com/2"},
        {"text": "نحن ندعم الدارجة", "url": "https://facebook.com/3"},
        {"text": "كيفاش تاع الحوال", "url": "https://facebook.com/4"},
    ]
    
    csv_mgr.add_records(records)
    
    print(f"Ajouté {len(records)} enregistrements\n")
    print("Sauvegarde du CSV...")
    output_file = csv_mgr.save_to_csv()
    
    print(f"✓ Fichier sauvegardé: {output_file}\n")
    
    # Statistiques
    stats = csv_mgr.get_statistics()
    print("Statistiques du CSV:")
    print(f"  Total records: {stats['total_records']}")
    print(f"  Unique URLs: {stats['unique_urls']}")
    print(f"  Avg text length: {stats['avg_text_length']:.2f}\n")


def example_full_pipeline():
    """Exemple du pipeline complet optimisé."""
    print("\n" + "="*60)
    print("EXEMPLE 5: Pipeline Complet Optimisé")
    print("="*60 + "\n")
    
    start_time = time.time()
    
    # Créer le pipeline
    pipeline = DarijaDatasetPipeline(use_scraper=False)
    
    # Générer des commentaires d'exemple
    comments = []
    sample_texts_darija = [
        "واح البدر يا سيدي! شحال ديال الجمالة",
        "مليح البزاف يا لخوي! غادي نشوفك قريب",
        "نحن ندعم اللغة الدارجة المغربية",
        "السلام عليكم ورحمة الله وبركاته",
        "كيفاش تاع الحوال يا صديقي",
        "واش كاين شي مشاكل؟",
        "ولاه يا سيدي، كلشي مليح",
        "غادي نتلاقاو قريب إن شاء الله",
        "شنو الأخبار يا صحابي",
        "حنا فقراء وعندنا لقمة العيش",
    ]
    
    for i, text in enumerate(sample_texts_darija * 5):  # 50 commentaires
        comments.append({
            "text": text + f" (comment {i+1})",
            "url": f"https://www.facebook.com/post/{i % 5 + 1}"
        })
    
    print(f"Traitement de {len(comments)} commentaires...")
    print(f"(Chaque texte sera nettoyé, filtré et vérifié)")
    print()
    
    # Traiter
    darija_count = pipeline.process_comments_batch(comments)
    
    # Sauvegarder
    output_file = pipeline.save_dataset()
    
    # Statistiques
    elapsed = time.time() - start_time
    pipeline.print_statistics()
    
    print(f"\n✓ Temps total: {elapsed:.2f}s")
    print(f"✓ Vitesse de traitement: {len(comments)/elapsed:.0f} comments/sec\n")


def example_performance_analysis():
    """Analyse détaillée de la performance."""
    print("\n" + "="*60)
    print("EXEMPLE 6: Analyse de Performance")
    print("="*60 + "\n")
    
    from src.cleaner import DataCleaner
    from src.language_detector import LanguageDetector
    import time
    
    cleaner = DataCleaner()
    detector = LanguageDetector()
    
    # Texte de test
    test_text = "واح البدر يا سيدي! شحال ديال الجمالة في هذا الوقت 😊"
    
    # Benchmark nettoyage
    start = time.time()
    for _ in range(1000):
        cleaner.clean(test_text)
    clean_time = time.time() - start
    
    # Benchmark détection
    cleaned = cleaner.clean(test_text)
    start = time.time()
    for _ in range(1000):
        detector.detect_language(cleaned)
    detect_time = time.time() - start
    
    print("Benchmark (1000 itérations):")
    print(f"  Nettoyage: {clean_time:.2f}s ({1000/clean_time:.0f} ops/sec)")
    print(f"  Détection: {detect_time:.2f}s ({1000/detect_time:.0f} ops/sec)")
    print()


def main():
    """Exécute tous les exemples."""
    print("\n" + "="*70)
    print(" "*15 + "DARIJA DATASET BUILDER - EXEMPLES AVANCÉS")
    print("="*70)
    
    try:
        # Example 1: Basic usage
        example_basic_usage()
        
        # Example 2: Optimized batching
        example_with_batching()
        
        # Example 3: Language detection with cache
        example_language_detection()
        
        # Example 4: CSV operations
        example_csv_operations()
        
        # Example 5: Full pipeline
        example_full_pipeline()
        
        # Example 6: Performance analysis
        example_performance_analysis()
        
        print("\n" + "="*70)
        print("✓ Tous les exemples ont été exécutés avec succès!")
        print("="*70 + "\n")
        
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution des exemples: {e}")
        raise


if __name__ == "__main__":
    main()
