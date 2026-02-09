import os
from pathlib import Path

# Répertoires principaux
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
MODELS_DIR = PROJECT_ROOT / "models"
LOGS_DIR = PROJECT_ROOT / "logs"
SRC_DIR = PROJECT_ROOT / "src"

# Créer les répertoires s'ils n'existent pas
DATA_DIR.mkdir(exist_ok=True)
MODELS_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# Configuration Facebook Scraping
FACEBOOK_CONFIG = {
    "headless": True,
    "disable_images": True,
    "disable_javascript": False,
    "timeout": 30,
    "page_load_timeout": 40,
    "implicit_wait": 10,
    "scroll_pause_time": 2,
    "max_retries": 3,
}

# Configuration nettoyage des données
CLEANING_CONFIG = {
    "remove_urls": True,
    "remove_emojis": True,            # Supprimer TOUS les emojis — dataset propre Darija uniquement
    "remove_punctuation": False,
    "remove_special_chars": True,     # Nettoyer les symboles inutiles
    "remove_html_tags": True,
    "normalize_whitespace": True,
    "min_comment_length": 5,          # Minimum 5 chars pour éviter le bruit
    "max_comment_length": 5000,
    "preserve_darija_latin": True,
}

# Configuration détection de langue
LANGUAGE_CONFIG = {
    "model_path": str(MODELS_DIR / "lid.176.ftz"),
    "model_url": "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.ftz",
    "darija_label": "__label__ar",
    "confidence_threshold": 0.4,      # Seuil relevé pour filtrage strict
    "batch_size": 5000,
    # Labels acceptés — arabe + dialecte égyptien (proche)
    "accepted_labels": ["__label__ar", "__label__arz"],
    # Activer la détection heuristique par mots-clés Darija
    "use_darija_heuristics": True,
    # Seuil minimum pour l'heuristique (nombre de mots Darija trouvés)
    "min_darija_words": 2,            # Minimum 2 mots Darija pour valider
    # Langues étrangères à rejeter explicitement
    "rejected_labels": ["__label__en", "__label__fr", "__label__es", "__label__tr", "__label__pl", "__label__de", "__label__it", "__label__pt", "__label__nl", "__label__ru", "__label__id", "__label__ms"],
}

# Configuration CSV
CSV_CONFIG = {
    "output_file": str(DATA_DIR / "darija_comments.csv"),
    "encoding": "utf-8-sig",       # BOM pour compatibilité Excel/Windows avec l'arabe
    "columns": ["text", "url"],
    "index": False,
}

# Configuration logging
LOGGING_CONFIG = {
    "log_file": str(LOGS_DIR / "darija_dataset.log"),
    "log_level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
}

# Configuration parallélisation
PARALLEL_CONFIG = {
    "max_workers": 8,
    "chunk_size": 500,
    "use_threading_for_scraping": True,
    "max_concurrent_posts": 3,
}

# Configuration Selenium (WebDriver)
WEBDRIVER_CONFIG = {
    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "window_size": "1920,1080",
}

# Configuration scraping étendue — pour dataset énorme
SCRAPING_CONFIG = {
    "scroll_times": 80,               # Scrolls max par page pour charger tous les commentaires
    "click_see_more": True,           # Cliquer sur "Voir plus de commentaires"
    "click_replies": True,            # Charger les réponses aux commentaires aussi
    "max_retries_per_post": 5,        # Tentatives max par publication
    "delay_between_posts": 2,         # Délai entre les publications (anti-ban)
    "delay_between_scrolls": 1.5,     # Délai entre scrolls
    "extract_replies": True,          # Extraire les sous-commentaires
    "max_comments_per_post": 0,       # 0 = illimité
}
