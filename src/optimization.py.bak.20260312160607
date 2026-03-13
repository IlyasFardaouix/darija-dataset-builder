"""
Module d'optimisation et cache pour améliorer les performances.
"""

from functools import lru_cache, wraps
from typing import Dict, Any, Callable
from collections import deque
import hashlib
import time

class CacheManager:
    """Gestionnaire de cache optimisé pour FastText et autres opérations coûteuses."""
    
    def __init__(self, max_size: int = 10000):
        """
        Initialise le gestionnaire de cache.
        
        Args:
            max_size: Taille maximale du cache
        """
        self.cache: Dict[str, Any] = {}
        self.max_size = max_size
        self.access_order = deque(maxlen=max_size)
        self.hits = 0
        self.misses = 0
    
    def _hash_key(self, text: str) -> str:
        """Génère une clé de hash pour un texte."""
        return hashlib.md5(text.encode()).hexdigest()
    
    def get(self, key: str) -> Any:
        """Récupère une valeur du cache."""
        if key in self.cache:
            self.hits += 1
            self.access_order.append(key)
            return self.cache[key]
        self.misses += 1
        return None
    
    def set(self, key: str, value: Any):
        """Définit une valeur dans le cache."""
        if len(self.cache) >= self.max_size:
            # Supprimer les éléments les moins utilisés (FIFO simple)
            oldest = self.access_order[0] if self.access_order else None
            if oldest and oldest in self.cache:
                del self.cache[oldest]
        
        self.cache[key] = value
        self.access_order.append(key)
    
    def clear(self):
        """Vide le cache."""
        self.cache.clear()
        self.access_order.clear()
        self.hits = 0
        self.misses = 0
    
    def get_stats(self) -> Dict[str, Any]:
        """Retourne les statistiques du cache."""
        total = self.hits + self.misses
        hit_rate = (self.hits / total * 100) if total > 0 else 0
        return {
            "size": len(self.cache),
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": hit_rate,
        }


class PerformanceMonitor:
    """Moniteur de performance pour identifier les goulots d'étranglement."""
    
    def __init__(self):
        """Initialise le moniteur."""
        self.timings: Dict[str, list] = {}
    
    def time_operation(self, operation_name: str):
        """Décorateur pour mesurer le temps d'une opération."""
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs):
                start = time.time()
                result = func(*args, **kwargs)
                elapsed = time.time() - start
                
                if operation_name not in self.timings:
                    self.timings[operation_name] = []
                self.timings[operation_name].append(elapsed)
                
                return result
            return wrapper
        return decorator
    
    def get_stats(self) -> Dict[str, Dict[str, float]]:
        """Retourne les statistiques de performance."""
        stats = {}
        for op_name, timings in self.timings.items():
            if timings:
                stats[op_name] = {
                    "count": len(timings),
                    "total_time": sum(timings),
                    "avg_time": sum(timings) / len(timings),
                    "min_time": min(timings),
                    "max_time": max(timings),
                }
        return stats
    
    def print_stats(self):
        """Affiche les statistiques de performance."""
        stats = self.get_stats()
        if not stats:
            print("Aucune donnée de performance")
            return
        
        print("\n" + "="*60)
        print("STATISTIQUES DE PERFORMANCE")
        print("="*60)
        
        for op_name, op_stats in stats.items():
            print(f"\n{op_name}:")
            print(f"  Exécutions: {op_stats['count']}")
            print(f"  Temps total: {op_stats['total_time']:.2f}s")
            print(f"  Temps moyen: {op_stats['avg_time']:.4f}s")
            print(f"  Min/Max: {op_stats['min_time']:.4f}s / {op_stats['max_time']:.4f}s")
        
        print("\n" + "="*60 + "\n")


class OptimizedBatchProcessor:
    """Processeur par lots optimisé pour maximiser l'efficacité."""
    
    @staticmethod
    def process_in_chunks(items: list, chunk_size: int = 100):
        """
        Divise une liste en chunks pour traitement optimisé.
        
        Args:
            items: Liste d'éléments
            chunk_size: Taille de chaque chunk
            
        Yields:
            Chunks de la taille spécifiée
        """
        for i in range(0, len(items), chunk_size):
            yield items[i:i + chunk_size]
    
    @staticmethod
    def process_with_batching(processor_func: Callable, items: list, 
                             batch_size: int = 100) -> list:
        """
        Traite des éléments par lots avec une fonction de processeur.
        
        Args:
            processor_func: Fonction qui traite un lot
            items: Liste d'éléments
            batch_size: Taille des lots
            
        Returns:
            Liste des résultats
        """
        results = []
        for chunk in OptimizedBatchProcessor.process_in_chunks(items, batch_size):
            results.extend(processor_func(chunk))
        return results


# Instances globales
cache_manager = CacheManager()
performance_monitor = PerformanceMonitor()
