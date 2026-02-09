import logging
import sys
from pathlib import Path
from config import LOGGING_CONFIG

def setup_logger(name: str) -> logging.Logger:
    """
    Configure et retourne un logger optimisé.
    
    Args:
        name: Nom du logger
        
    Returns:
        Logger configuré
    """
    logger = logging.getLogger(name)
    
    if logger.hasHandlers():
        return logger
    
    logger.setLevel(getattr(logging, LOGGING_CONFIG["log_level"]))
    
    # Handler fichier
    log_file = Path(LOGGING_CONFIG["log_file"])
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(getattr(logging, LOGGING_CONFIG["log_level"]))
    
    # Handler console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    # Format
    formatter = logging.Formatter(LOGGING_CONFIG["format"])
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger
