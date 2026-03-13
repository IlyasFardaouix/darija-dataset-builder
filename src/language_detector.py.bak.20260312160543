import fasttext
import os
from pathlib import Path
from typing import Tuple, List
from urllib.request import urlretrieve
from config import LANGUAGE_CONFIG, MODELS_DIR
from src.logger import setup_logger
from src.optimization import cache_manager
from src.darija_wordbank import score_darija, is_darija_heuristic, has_arabic_script

logger = setup_logger(__name__)


class LanguageDetector:
    """
    Détecteur de langue Darija OPTIMISÉ — combinaison FastText + heuristiques Darija.
    
    Stratégie de détection (pour maximiser le nombre de commentaires Darija):
    1. FastText détecte si c'est de l'arabe (__label__ar) → score confiance
    2. Heuristique Darija: cherche des mots/expressions spécifiquement marocains
    3. Un commentaire est considéré Darija si:
       - FastText dit arabe AVEC confiance >= seuil (même bas: 0.25)
       - OU l'heuristique détecte >= 1 mot Darija connu
       - OU le texte contient de l'écriture arabe ET des mots Darija
    
    Cela capture beaucoup plus de commentaires que FastText seul,
    car la Darija mélange arabe, français et latin.
    """
    
    def __init__(self):
        """Initialise le détecteur et télécharge le modèle si nécessaire."""
        self.model_path = Path(LANGUAGE_CONFIG["model_path"])
        self.model = None
        self.use_heuristics = LANGUAGE_CONFIG.get("use_darija_heuristics", True)
        self.accepted_labels = LANGUAGE_CONFIG.get("accepted_labels", ["__label__ar"])
        self.min_darija_words = LANGUAGE_CONFIG.get("min_darija_words", 1)
        self._load_model()
    
    def _load_model(self):
        """Charge le modèle FastText, le télécharge si nécessaire."""
        if not self.model_path.exists():
            logger.info("Téléchargement du modèle FastText...")
            MODELS_DIR.mkdir(parents=True, exist_ok=True)
            try:
                urlretrieve(
                    LANGUAGE_CONFIG["model_url"],
                    str(self.model_path)
                )
                logger.info("Modèle téléchargé avec succès")
            except Exception as e:
                logger.error(f"Erreur lors du téléchargement du modèle: {e}")
                raise
        
        try:
            # Charger le modèle — supprimer les warnings FastText
            import warnings
            warnings.filterwarnings('ignore')
            self.model = fasttext.load_model(str(self.model_path))
            logger.info("Modèle FastText chargé avec succès")
        except Exception as e:
            logger.error(f"Erreur lors du chargement du modèle: {e}")
            raise
    
    def detect_language(self, text: str) -> Tuple[str, float]:
        """
        Détecte la langue d'un texte avec cache.
        
        Args:
            text: Texte à analyser
            
        Returns:
            Tuple (langue_label, score_confiance)
        """
        if not text or not isinstance(text, str) or len(text.strip()) == 0:
            return ("unknown", 0.0)
        
        # Vérifier le cache
        cache_key = hash(text)
        cached_result = cache_manager.get(str(cache_key))
        if cached_result:
            return cached_result
        
        try:
            # Nettoyer les caractères spéciaux pour FastText
            clean_text = text.replace('\n', ' ').replace('\t', ' ')
            # Prédire les top 3 langues pour avoir plus de contexte
            predictions = self.model.predict(clean_text, k=3)
            
            if predictions and predictions[0] and predictions[1]:
                language = predictions[0][0]
                confidence = float(predictions[1][0])
                result = (language, confidence)
                # Mettre en cache
                cache_manager.set(str(cache_key), result)
                return result
            return ("unknown", 0.0)
        except Exception as e:
            logger.debug(f"Erreur détection langue: {e}")
            return ("unknown", 0.0)
    
    def is_darija(self, text: str, threshold: float = None) -> bool:
        """
        Vérifie si le texte est en Darija — DÉTECTION STRICTE.
        
        Filtre en 2 étapes:
        1. REJETER si FastText identifie clairement une langue étrangère (en, fr, tr, pl, etc.)
           SAUF si le texte contient des mots Darija connus
        2. ACCEPTER si :
           - FastText dit arabe AVEC confiance >= seuil
           - OU heuristique détecte >= 2 mots Darija (arabe OU latin)
           - OU texte contient script arabe + au moins 1 mot Darija
        
        Args:
            text: Texte à analyser
            threshold: Seuil de confiance (défaut: config = 0.4)
            
        Returns:
            True si le texte est considéré comme Darija
        """
        if not text or not isinstance(text, str) or len(text.strip()) < 3:
            return False
        
        if threshold is None:
            threshold = LANGUAGE_CONFIG["confidence_threshold"]
        
        # === Analyse heuristique Darija (arabe ET latin) ===
        darija_word_count = 0
        heuristic_says_darija = False
        if self.use_heuristics:
            _, darija_word_count = score_darija(text)
            heuristic_says_darija = darija_word_count >= self.min_darija_words
        
        # === FastText detection ===
        language, confidence = self.detect_language(text)
        rejected_labels = LANGUAGE_CONFIG.get("rejected_labels", [])
        
        # Si FastText est confiant que c'est une langue étrangère
        # → REJETER sauf si l'heuristique trouve des mots Darija
        if language in rejected_labels and confidence >= 0.3:
            if not heuristic_says_darija:
                return False
        
        # === Vérification positive ===
        arabic_present = has_arabic_script(text)
        
        # Stratégie A: FastText dit arabe avec confiance suffisante
        fasttext_says_arabic = (
            language in self.accepted_labels and confidence >= threshold
        )
        
        # Stratégie B: Script arabe + au moins 1 mot Darija
        arabic_with_darija = arabic_present and darija_word_count >= 1
        
        # Décision finale
        return fasttext_says_arabic or heuristic_says_darija or arabic_with_darija
    
    def detect_batch(self, texts: List[str]) -> List[Tuple[str, float]]:
        """
        Détecte la langue pour un lot de textes (optimisé avec parallélisation).
        
        Args:
            texts: Liste de textes
            
        Returns:
            Liste de tuples (langue, confiance)
        """
        from src.optimization import OptimizedBatchProcessor
        
        def batch_processor(batch):
            return [self.detect_language(text) for text in batch]
        
        return OptimizedBatchProcessor.process_with_batching(
            batch_processor, texts, batch_size=500
        )
    
    def filter_darija(self, texts: List[str], threshold: float = None) -> List[str]:
        """
        Filtre une liste de textes pour garder uniquement ceux en Darija.
        
        Args:
            texts: Liste de textes
            threshold: Seuil de confiance
            
        Returns:
            Liste filtrée contenant uniquement les textes Darija
        """
        if threshold is None:
            threshold = LANGUAGE_CONFIG["confidence_threshold"]
        
        darija_texts = []
        for text in texts:
            if self.is_darija(text, threshold):
                darija_texts.append(text)
        
        logger.info(f"Filtré {len(darija_texts)}/{len(texts)} textes comme Darija")
        return darija_texts
    
    def get_darija_details(self, text: str) -> dict:
        """
        Retourne des détails complets sur la détection Darija d'un texte.
        Utile pour le debugging et l'analyse.
        
        Args:
            text: Texte à analyser
            
        Returns:
            Dict avec tous les détails de détection
        """
        language, confidence = self.detect_language(text)
        darija_score, darija_word_count = score_darija(text)
        
        return {
            "text": text[:80] + "..." if len(text) > 80 else text,
            "fasttext_label": language,
            "fasttext_confidence": round(confidence, 4),
            "darija_score": round(darija_score, 4),
            "darija_word_count": darija_word_count,
            "has_arabic_script": has_arabic_script(text),
            "is_darija": self.is_darija(text),
        }
