"""
Module d'initialisation pour les modules source.
Darija Dataset Builder — Architecture Multi-Pipeline (~1M commentaires).

Les imports sont lazy pour éviter les erreurs si des dépendances ne sont pas installées.
"""

# Imports sans dépendances externes
from src.logger import setup_logger
from src.darija_wordbank import score_darija, is_darija_heuristic

__all__ = [
    "setup_logger",
    "score_darija",
    "is_darija_heuristic",
]

# Imports avec dépendances — disponibles uniquement si les packages sont installés
def __getattr__(name):
    """Import lazy pour les modules avec dépendances externes."""
    if name == "DataCleaner":
        from src.cleaner import DataCleaner
        return DataCleaner
    elif name == "LanguageDetector":
        from src.language_detector import LanguageDetector
        return LanguageDetector
    elif name == "CSVManager":
        from src.csv_manager import CSVManager
        return CSVManager
    elif name == "FacebookScraper":
        from src.facebook_scraper import FacebookScraper
        return FacebookScraper
    elif name == "DarijaDatasetPipeline":
        from src.pipeline import DarijaDatasetPipeline
        return DarijaDatasetPipeline
    elif name == "generate_dataset_list":
        from src.darija_dataset_generator import generate_dataset_list
        return generate_dataset_list
    elif name == "generate_massive_dataset":
        from src.darija_dataset_generator import generate_massive_dataset
        return generate_massive_dataset
    elif name == "YouTubeScraper":
        from src.youtube_scraper import YouTubeScraper
        return YouTubeScraper
    elif name == "HespressScraper":
        from src.hespress_scraper import HespressScraper
        return HespressScraper
    elif name == "TikTokScraper":
        from src.tiktok_scraper import TikTokScraper
        return TikTokScraper
    elif name == "TwitterScraper":
        from src.twitter_scraper import TwitterScraper
        return TwitterScraper
    elif name == "MergePipeline":
        from src.merge_pipeline import MergePipeline
        return MergePipeline
    raise AttributeError(f"module 'src' has no attribute {name}")
