"""
Configuration avancée et profils de performance.
"""

from enum import Enum
from dataclasses import dataclass
from typing import Dict

class DatasetSize(Enum):
    """Enum pour les tailles de dataset."""
    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"
    XLARGE = "xlarge"


@dataclass
class PerformanceProfile:
    """Profil de performance optimisé."""
    
    name: str
    batch_size: int
    cache_size: int
    max_workers: int
    chunk_size: int
    fasttext_batch: int
    memory_efficient: bool
    use_gpu: bool
    
    def __str__(self):
        return f"""
Performance Profile: {self.name}
  - Batch Size: {self.batch_size}
  - Cache Size: {self.cache_size}
  - Max Workers: {self.max_workers}
  - Chunk Size: {self.chunk_size}
  - FastText Batch: {self.fasttext_batch}
  - Memory Efficient: {self.memory_efficient}
  - Use GPU: {self.use_gpu}
"""


# Profils prédéfinis
PROFILES = {
    DatasetSize.SMALL: PerformanceProfile(
        name="Small Dataset (< 1000 records)",
        batch_size=50,
        cache_size=1000,
        max_workers=1,
        chunk_size=50,
        fasttext_batch=100,
        memory_efficient=False,
        use_gpu=False,
    ),
    DatasetSize.MEDIUM: PerformanceProfile(
        name="Medium Dataset (1000-10000 records)",
        batch_size=200,
        cache_size=5000,
        max_workers=4,
        chunk_size=200,
        fasttext_batch=500,
        memory_efficient=True,
        use_gpu=False,
    ),
    DatasetSize.LARGE: PerformanceProfile(
        name="Large Dataset (10000-100000 records)",
        batch_size=1000,
        cache_size=20000,
        max_workers=8,
        chunk_size=1000,
        fasttext_batch=2000,
        memory_efficient=True,
        use_gpu=True,
    ),
    DatasetSize.XLARGE: PerformanceProfile(
        name="XLarge Dataset (> 100000 records)",
        batch_size=5000,
        cache_size=50000,
        max_workers=16,
        chunk_size=5000,
        fasttext_batch=10000,
        memory_efficient=True,
        use_gpu=True,
    ),
}


def get_profile(size: DatasetSize) -> PerformanceProfile:
    """
    Retourne un profil de performance.
    
    Args:
        size: Taille du dataset
        
    Returns:
        Profil de performance
    """
    return PROFILES.get(size, PROFILES[DatasetSize.MEDIUM])


# Configuration avancée
ADVANCED_CONFIG = {
    "enable_caching": True,
    "enable_compression": True,
    "enable_deduplication": True,
    "enable_logging": True,
    "enable_benchmarking": False,
    
    # Limites
    "max_cache_entries": 20000,
    "max_batch_size": 10000,
    "max_workers": 8,
    
    # Timeouts
    "request_timeout": 30,
    "page_load_timeout": 40,
    "scroll_timeout": 60,
    
    # Seuils
    "min_confidence": 0.5,
    "min_text_length": 5,
    "max_text_length": 1000,
    
    # Options de scraping
    "headless_mode": True,
    "disable_images": True,
    "disable_css": False,
    "disable_javascript": False,
    
    # Optimisations
    "use_memory_mapping": True,
    "use_multiprocessing": False,
    "use_gpu_fasttext": False,
    "parallel_posts": True,
    
    # Compression
    "compress_output": False,
    "compression_level": 9,
}


def apply_profile(profile: PerformanceProfile) -> Dict:
    """
    Applique un profil à la configuration.
    
    Args:
        profile: Profil de performance
        
    Returns:
        Configuration mise à jour
    """
    config = ADVANCED_CONFIG.copy()
    
    config["max_batch_size"] = profile.batch_size
    config["max_cache_entries"] = profile.cache_size
    config["max_workers"] = profile.max_workers
    config["use_gpu_fasttext"] = profile.use_gpu
    
    if profile.memory_efficient:
        config["max_batch_size"] = min(500, profile.batch_size)
        config["use_memory_mapping"] = True
    
    return config
