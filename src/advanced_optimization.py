"""
Optimisations avancées pour performance maximale.
"""

import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import List, Callable, Any
import os
from src.logger import setup_logger

logger = setup_logger(__name__)


class ParallelProcessor:
    """Processeur parallélisé pour exploiter multi-core."""
    
    def __init__(self, use_multiprocessing: bool = False):
        """
        Initialise le processeur.
        
        Args:
            use_multiprocessing: Utiliser multiprocessing (vs threading)
        """
        self.use_multiprocessing = use_multiprocessing
        self.max_workers = os.cpu_count() or 4
    
    def process_parallel(self, items: List[Any], func: Callable, 
                        chunk_size: int = 100) -> List[Any]:
        """
        Traite des éléments en parallèle.
        
        Args:
            items: Liste d'éléments à traiter
            func: Fonction de traitement
            chunk_size: Taille de chaque chunk
            
        Returns:
            Liste des résultats
        """
        ExecutorClass = ProcessPoolExecutor if self.use_multiprocessing else ThreadPoolExecutor
        
        results = []
        try:
            with ExecutorClass(max_workers=self.max_workers) as executor:
                futures = []
                
                for i in range(0, len(items), chunk_size):
                    chunk = items[i:i + chunk_size]
                    future = executor.submit(func, chunk)
                    futures.append(future)
                
                for future in futures:
                    try:
                        result = future.result(timeout=300)
                        if isinstance(result, list):
                            results.extend(result)
                        else:
                            results.append(result)
                    except Exception as e:
                        logger.error(f"Erreur lors du traitement parallèle: {e}")
        
        except Exception as e:
            logger.error(f"Erreur: {e}")
        
        return results


class MemoryOptimizer:
    """Optimiseur mémoire pour réduire la consommation."""
    
    @staticmethod
    def process_large_file(filepath: str, processor_func: Callable, 
                          chunk_size: int = 1000):
        """
        Traite un fichier volumineux par chunks.
        
        Args:
            filepath: Chemin du fichier
            processor_func: Fonction pour traiter chaque chunk
            chunk_size: Taille du chunk en lignes
            
        Yields:
            Résultats du traitement
        """
        import pandas as pd
        
        try:
            for chunk in pd.read_csv(filepath, chunksize=chunk_size):
                result = processor_func(chunk)
                yield result
        except Exception as e:
            logger.error(f"Erreur lecture fichier: {e}")
    
    @staticmethod
    def release_memory():
        """Libère la mémoire inutilisée."""
        import gc
        gc.collect()


class FastTextOptimizations:
    """Optimisations spécifiques à FastText."""
    
    @staticmethod
    def batch_predict(model, texts: List[str], batch_size: int = 1000) -> List:
        """
        Prédictions par lots optimisées.
        
        Args:
            model: Modèle FastText
            texts: Textes à prédire
            batch_size: Taille du lot
            
        Returns:
            Prédictions
        """
        predictions = []
        
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            # FastText peut traiter plusieurs textes à la fois
            for text in batch:
                pred = model.predict(text)
                predictions.append(pred)
        
        return predictions
    
    @staticmethod
    def optimize_model_loading(model_path: str):
        """
        Optimise le chargement du modèle FastText.
        
        Args:
            model_path: Chemin du modèle
            
        Returns:
            Modèle chargé
        """
        import fasttext
        import warnings
        
        # Supprimer les avertissements
        warnings.filterwarnings('ignore')
        
        try:
            model = fasttext.load_model(model_path)
            logger.info(f"Modèle FastText chargé: {model_path}")
            return model
        except Exception as e:
            logger.error(f"Erreur: {e}")
            raise


class StringOptimizations:
    """Optimisations de traitement de chaînes."""
    
    @staticmethod
    def fast_unicode_normalize(text: str) -> str:
        """
        Normalise Unicode rapidement.
        
        Args:
            text: Texte à normaliser
            
        Returns:
            Texte normalisé
        """
        import unicodedata
        # Utiliser NFC (compose) au lieu de NFD (décompose)
        return unicodedata.normalize('NFC', text)
    
    @staticmethod
    def deduplicate_texts(texts: List[str]) -> List[str]:
        """
        Supprime les doublons en conservant l'ordre.
        
        Args:
            texts: Liste de textes
            
        Returns:
            Liste sans doublons
        """
        seen = set()
        unique = []
        
        for text in texts:
            # Utiliser hash pour comparaison rapide
            text_hash = hash(text)
            if text_hash not in seen:
                seen.add(text_hash)
                unique.append(text)
        
        return unique


class QueryOptimizer:
    """Optimiseur pour requêtes Facebook."""
    
    @staticmethod
    def optimize_selectors() -> dict:
        """
        Retourne les sélecteurs CSS optimisés.
        
        Returns:
            Dict de sélecteurs efficaces
        """
        return {
            "comments": [
                'div[data-testid="UFI2CommentBody_Comment_Text"]',
                'div[data-testid="commentText"]',
                'div.xod5an3',
                'span.xod5an3',
            ],
            "comment_author": [
                'a[data-testid="comment_author_name"]',
                'span.x1whsf0u',
            ],
            "comment_time": [
                'a[href*="comment_id"]',
                'abbr[data-testid*="time"]',
            ]
        }


class CompressionUtils:
    """Utilitaires pour compression de données."""
    
    @staticmethod
    def compress_csv(input_file: str, output_file: str = None):
        """
        Compresse un fichier CSV en gzip.
        
        Args:
            input_file: Fichier d'entrée
            output_file: Fichier de sortie compressé
        """
        import gzip
        import shutil
        
        output_file = output_file or input_file + '.gz'
        
        try:
            with open(input_file, 'rb') as f_in:
                with gzip.open(output_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            logger.info(f"Fichier compressé: {output_file}")
            return output_file
        except Exception as e:
            logger.error(f"Erreur compression: {e}")
            return None


class ConfigOptimizer:
    """Optimisation des configurations."""
    
    @staticmethod
    def get_optimized_config(dataset_size: str = "medium") -> dict:
        """
        Retourne une configuration optimisée selon la taille du dataset.
        
        Args:
            dataset_size: "small", "medium", "large"
            
        Returns:
            Configuration optimisée
        """
        configs = {
            "small": {
                "batch_size": 50,
                "cache_size": 1000,
                "max_workers": 2,
                "chunk_size": 50,
            },
            "medium": {
                "batch_size": 200,
                "cache_size": 5000,
                "max_workers": 4,
                "chunk_size": 200,
            },
            "large": {
                "batch_size": 1000,
                "cache_size": 20000,
                "max_workers": 8,
                "chunk_size": 1000,
            }
        }
        
        return configs.get(dataset_size, configs["medium"])
