import re
import unicodedata
from typing import List
from config import CLEANING_CONFIG
from src.logger import setup_logger

logger = setup_logger(__name__)


class DataCleaner:
    """
    Nettoyeur de données OPTIMISÉ pour les commentaires Darija marocains.
    
    IMPORTANT pour la Darija:
    - NE PAS supprimer les chiffres 3, 7, 9, 5, 8, 2 → ils remplacent des sons arabes
      (3=ع, 7=ح, 9=ق, 5=خ, 8=غ, 2=ء)
    - NE PAS supprimer les emojis par défaut → ils font partie du langage social
    - Garder les caractères arabes, latins et les chiffres mixtes
    - Normaliser les espaces mais garder la ponctuation arabe
    """
    
    # Regex pré-compilées pour performance maximale
    URL_PATTERN = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
    HTML_PATTERN = re.compile(r'<[^>]+>')
    EMOJI_PATTERN = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # Emoticones
        "\U0001F300-\U0001F5FF"  # Symboles
        "\U0001F680-\U0001F6FF"  # Transport
        "\U0001F1E0-\U0001F1FF"  # Drapeaux
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "\U0001f926-\U0001f937"
        "\U00010000-\U0010ffff"
        "\u2640-\u2642"
        "\u2600-\u2B55"
        "\u200d"
        "\u23cf"
        "\u23e9"
        "\u231a"
        "\ufe0f"
        "\u3030"
        "]+", re.UNICODE
    )
    WHITESPACE_PATTERN = re.compile(r'\s+')
    
    # Pattern spécial: supprimer UNIQUEMENT les caractères vraiment inutiles
    # Garder: arabe, latin, chiffres, espaces, ponctuation arabe (؟ ، ؛)
    JUNK_CHARS_PATTERN = re.compile(
        r'[^\u0600-\u06FF\u0750-\u077F\uFB50-\uFDFF\uFE70-\uFEFF'
        r'a-zA-Z0-9\s'
        r'.,!?;:\'\"\-\(\)'
        r'،؟؛'  # Ponctuation arabe
        r']'
    )
    
    @staticmethod
    def remove_urls(text: str) -> str:
        """Supprime les URLs."""
        return DataCleaner.URL_PATTERN.sub('', text)
    
    @staticmethod
    def remove_html_tags(text: str) -> str:
        """Supprime les balises HTML."""
        return DataCleaner.HTML_PATTERN.sub('', text)
    
    @staticmethod
    def remove_emojis(text: str) -> str:
        """Supprime les emojis (optionnel — par défaut désactivé pour Darija)."""
        return DataCleaner.EMOJI_PATTERN.sub('', text)
    
    @staticmethod
    def remove_special_chars(text: str) -> str:
        """
        Supprime les caractères spéciaux TOUT EN GARDANT:
        - Caractères arabes (toute la plage Unicode arabe)
        - Lettres latines (a-z, A-Z) — utilisées en Darija latine
        - Chiffres (0-9) — CRITIQUE: 3,7,9 sont des sons arabes en Darija latine
        - Ponctuation arabe (،؟؛)
        """
        cleaned = DataCleaner.JUNK_CHARS_PATTERN.sub(' ', text)
        return cleaned
    
    @staticmethod
    def normalize_whitespace(text: str) -> str:
        """Normalise les espaces."""
        text = DataCleaner.WHITESPACE_PATTERN.sub(' ', text)
        return text.strip()
    
    @staticmethod
    def normalize_unicode(text: str) -> str:
        """Normalise les caractères Unicode (NFC = forme composée)."""
        return unicodedata.normalize('NFC', text)
    
    @staticmethod
    def remove_repeated_chars(text: str) -> str:
        """
        Réduit les caractères répétés (ex: "واااااو" → "وااو", "hhhhh" → "hh").
        Courant dans les commentaires Facebook Darija.
        """
        # Réduire les répétitions à max 2 caractères
        return re.sub(r'(.)\1{2,}', r'\1\1', text)
    
    @staticmethod
    def is_valid_length(text: str) -> bool:
        """Vérifie si la longueur du texte est valide."""
        length = len(text.strip())
        return (CLEANING_CONFIG["min_comment_length"] <= length <= 
                CLEANING_CONFIG["max_comment_length"])
    
    @classmethod
    def clean(cls, text: str) -> str:
        """
        Nettoie le texte en appliquant toutes les transformations.
        Pipeline de nettoyage optimisé pour Darija.
        
        Args:
            text: Texte à nettoyer
            
        Returns:
            Texte nettoyé
        """
        if not text or not isinstance(text, str):
            return ""
        
        # Normaliser Unicode
        text = cls.normalize_unicode(text)
        
        # Supprimer les balises HTML
        if CLEANING_CONFIG.get("remove_html_tags", True):
            text = cls.remove_html_tags(text)
        
        # Supprimer les URLs
        if CLEANING_CONFIG.get("remove_urls", True):
            text = cls.remove_urls(text)
        
        # Supprimer les emojis (désactivé par défaut pour Darija)
        if CLEANING_CONFIG.get("remove_emojis", False):
            text = cls.remove_emojis(text)
        
        # Supprimer les caractères spéciaux (mode safe pour Darija)
        if CLEANING_CONFIG.get("remove_special_chars", False):
            text = cls.remove_special_chars(text)
        
        # Réduire les caractères répétés (waaaaaw → waaw)
        text = cls.remove_repeated_chars(text)
        
        # Normaliser les espaces
        if CLEANING_CONFIG.get("normalize_whitespace", True):
            text = cls.normalize_whitespace(text)
        
        return text
    
    @classmethod
    def clean_batch(cls, texts: List[str]) -> List[str]:
        """
        Nettoie un lot de textes (optimisé).
        
        Args:
            texts: Liste de textes
            
        Returns:
            Liste de textes nettoyés et filtrés (les vides sont retirés)
        """
        cleaned = []
        for text in texts:
            c = cls.clean(text)
            if c and len(c.strip()) > 0:
                cleaned.append(c)
        return cleaned
