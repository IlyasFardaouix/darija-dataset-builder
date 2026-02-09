"""
Tests unitaires pour le projet Darija Dataset.
Couvre: cleaner, language_detector (heuristiques), csv_manager,
        darija_wordbank, darija_dataset_generator, optimization.
"""

import unittest
import sys
from pathlib import Path

# Ajouter le répertoire parent au chemin
sys.path.insert(0, str(Path(__file__).parent))

from src.cleaner import DataCleaner
from src.csv_manager import CSVManager
from src.darija_wordbank import score_darija, is_darija_heuristic, has_arabic_script
from src.darija_dataset_generator import (
    generate_dataset_list, get_all_base_comments, generate_variation
)


class TestDataCleaner(unittest.TestCase):
    """Tests pour DataCleaner optimisé Darija."""
    
    def setUp(self):
        self.cleaner = DataCleaner()
    
    def test_remove_urls(self):
        """Test suppression d'URLs."""
        text = "مليح البزاف https://example.com يا لخوي"
        cleaned = self.cleaner.remove_urls(text)
        self.assertNotIn("https", cleaned)
    
    def test_remove_emojis(self):
        """Test que les emojis sont supprimés (config: remove_emojis=True)."""
        text = "مليح البزاف 😊 يا لخوي"
        cleaned = self.cleaner.clean(text)
        self.assertNotIn("😊", cleaned)
    
    def test_preserve_darija_latin_numbers(self):
        """Test que les chiffres Darija latine (3, 7, 9) sont préservés."""
        text = "3afak goli 7aja zwina 9rit"
        cleaned = self.cleaner.clean(text)
        self.assertIn("3", cleaned)
        self.assertIn("7", cleaned)
        self.assertIn("9", cleaned)
    
    def test_preserve_arabic_chars(self):
        """Test que les caractères arabes sont préservés."""
        text = "واح البدر يا سيدي شحال ديال الجمالة"
        cleaned = self.cleaner.clean(text)
        self.assertIn("واح", cleaned)
        self.assertIn("ديال", cleaned)
    
    def test_reduce_repeated_chars(self):
        """Test réduction des caractères répétés."""
        text = "واااااو مليييييح بزااااف"
        cleaned = self.cleaner.clean(text)
        self.assertNotIn("ااااا", cleaned)
    
    def test_clean_html(self):
        """Test suppression HTML."""
        text = "<b>مليح</b> <a href='x'>البزاف</a>"
        cleaned = self.cleaner.clean(text)
        self.assertNotIn("<b>", cleaned)
        self.assertNotIn("<a", cleaned)
    
    def test_is_valid_length(self):
        """Test validation de longueur (min=3, max=5000)."""
        self.assertFalse(self.cleaner.is_valid_length("مل"))      # Trop court (2 chars)
        self.assertTrue(self.cleaner.is_valid_length("مليح"))      # OK (4 chars)
        self.assertTrue(self.cleaner.is_valid_length("مليح البزاف يا لخوي"))  # OK
    
    def test_clean_batch(self):
        """Test nettoyage par lots."""
        texts = ["مليح https://x.com", "بخير والله", ""]
        cleaned = self.cleaner.clean_batch(texts)
        self.assertEqual(len(cleaned), 2)  # Le vide est retiré


class TestDarijaWordbank(unittest.TestCase):
    """Tests pour la banque de mots Darija et les heuristiques."""
    
    def test_score_darija_arabic(self):
        """Test scoring avec texte Darija en arabe."""
        text = "واش كاين شي حاجة جديدة يا لخوي"
        score, count = score_darija(text)
        self.assertGreater(count, 0, "Devrait trouver des mots Darija")
        self.assertGreater(score, 0)
    
    def test_score_darija_latin(self):
        """Test scoring avec Darija latine."""
        text = "wach labas 3lik a sahbi kifach"
        score, count = score_darija(text)
        self.assertGreater(count, 0, "Devrait trouver des mots Darija latins")
    
    def test_score_english(self):
        """Test scoring avec texte anglais (devrait être bas)."""
        text = "This is a regular English comment"
        score, count = score_darija(text)
        self.assertEqual(count, 0, "Pas de mots Darija en anglais")
    
    def test_is_darija_heuristic(self):
        """Test heuristique Darija."""
        self.assertTrue(is_darija_heuristic("مليح البزاف يا لخوي"))
        self.assertTrue(is_darija_heuristic("wach labas 3lik"))
        self.assertFalse(is_darija_heuristic("Hello world"))
    
    def test_has_arabic_script(self):
        """Test détection script arabe."""
        self.assertTrue(has_arabic_script("مرحبا"))
        self.assertFalse(has_arabic_script("Hello"))
        self.assertTrue(has_arabic_script("mix مرحبا text"))
    
    def test_empty_text(self):
        """Test avec texte vide."""
        score, count = score_darija("")
        self.assertEqual(score, 0.0)
        self.assertEqual(count, 0)


class TestDatasetGenerator(unittest.TestCase):
    """Tests pour le générateur de dataset massif."""
    
    def test_base_comments_count(self):
        """Test que la banque de base a 300+ commentaires."""
        base = get_all_base_comments()
        self.assertGreaterEqual(len(base), 300,
            f"La banque devrait avoir 300+ commentaires, a {len(base)}")
    
    def test_generate_small_dataset(self):
        """Test génération d'un petit dataset."""
        dataset = generate_dataset_list(target_size=100)
        self.assertEqual(len(dataset), 100)
        
        # Vérifier la structure
        for item in dataset[:5]:
            self.assertIn("text", item)
            self.assertIn("url", item)
            self.assertIsInstance(item["text"], str)
            self.assertTrue(len(item["text"]) > 0)
    
    def test_generate_variation(self):
        """Test que les variations diffèrent parfois."""
        text = "مليح البزاف يا لخوي"
        variations = set()
        for _ in range(50):
            v = generate_variation(text)
            variations.add(v)
        # Au moins quelques variations différentes
        self.assertGreater(len(variations), 1)
    
    def test_dataset_diversity(self):
        """Test diversité du dataset généré."""
        dataset = generate_dataset_list(target_size=500)
        texts = set(item["text"] for item in dataset)
        urls = set(item["url"] for item in dataset)
        
        # Au moins 100 textes uniques sur 500
        self.assertGreater(len(texts), 100)
        # Plusieurs URLs différentes
        self.assertGreater(len(urls), 5)


class TestCSVManager(unittest.TestCase):
    """Tests pour CSVManager."""
    
    def setUp(self):
        self.csv_mgr = CSVManager("data/test_output.csv")
    
    def test_add_record(self):
        """Test ajout d'enregistrement."""
        self.csv_mgr.add_record("مليح البزاف", "https://facebook.com/1")
        self.assertEqual(len(self.csv_mgr.data), 1)
    
    def test_add_records(self):
        """Test ajout multiple d'enregistrements."""
        records = [
            {"text": "مليح", "url": "https://facebook.com/1"},
            {"text": "واح", "url": "https://facebook.com/2"},
        ]
        self.csv_mgr.add_records(records)
        self.assertEqual(len(self.csv_mgr.data), 2)
    
    def test_get_statistics(self):
        """Test statistiques."""
        self.csv_mgr.add_record("مليح البزاف", "https://facebook.com/1")
        self.csv_mgr.add_record("واح البدر", "https://facebook.com/1")
        
        stats = self.csv_mgr.get_statistics()
        self.assertEqual(stats["total_records"], 2)
        self.assertEqual(stats["unique_urls"], 1)
    
    def test_empty_stats(self):
        """Test statistiques vides."""
        stats = self.csv_mgr.get_statistics()
        self.assertEqual(stats["total_records"], 0)
    
    def tearDown(self):
        """Nettoyer les fichiers de test."""
        import os
        if os.path.exists("data/test_output.csv"):
            os.remove("data/test_output.csv")


class TestOptimization(unittest.TestCase):
    """Tests pour les optimisations."""
    
    def test_cache_manager(self):
        """Test gestionnaire de cache."""
        from src.optimization import CacheManager
        
        cache = CacheManager()
        cache.set("key1", "value1")
        self.assertEqual(cache.get("key1"), "value1")
        self.assertIsNone(cache.get("key2"))
    
    def test_cache_stats(self):
        """Test statistiques du cache."""
        from src.optimization import CacheManager
        
        cache = CacheManager()
        cache.set("k1", "v1")
        cache.get("k1")  # hit
        cache.get("k2")  # miss
        
        stats = cache.get_stats()
        self.assertEqual(stats["hits"], 1)
        self.assertEqual(stats["misses"], 1)
    
    def test_batch_processor(self):
        """Test processeur par lots."""
        from src.optimization import OptimizedBatchProcessor
        
        items = list(range(100))
        chunks = list(OptimizedBatchProcessor.process_in_chunks(items, 25))
        
        self.assertEqual(len(chunks), 4)
        self.assertEqual(len(chunks[0]), 25)


def run_tests():
    """Exécute tous les tests."""
    print("\n" + "="*60)
    print("🧪 TESTS UNITAIRES - DARIJA DATASET BUILDER")
    print("="*60 + "\n")
    unittest.main(argv=[''], exit=False, verbosity=2)


if __name__ == "__main__":
    run_tests()
