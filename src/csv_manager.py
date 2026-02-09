# -*- coding: utf-8 -*-
import pandas as pd
from pathlib import Path
from typing import List, Dict, Tuple
from config import CSV_CONFIG
from src.logger import setup_logger

logger = setup_logger(__name__)

class CSVManager:
    """Gestionnaire d'export et d'import de données CSV optimisé."""
    
    def __init__(self, output_file: str = None):
        """
        Initialise le gestionnaire CSV.
        
        Args:
            output_file: Chemin du fichier CSV (défaut: config)
        """
        self.output_file = Path(output_file or CSV_CONFIG["output_file"])
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        self.data = []
    
    def add_record(self, text: str, url: str):
        """
        Ajoute un enregistrement.
        
        Args:
            text: Texte du commentaire
            url: URL du post
        """
        if text and url:
            self.data.append({
                "text": text.strip(),
                "url": url.strip()
            })
    
    def add_records(self, records: List[Dict[str, str]]):
        """
        Ajoute plusieurs enregistrements.
        
        Args:
            records: Liste de dictionnaires avec 'text' et 'url'
        """
        for record in records:
            if "text" in record and "url" in record:
                self.add_record(record["text"], record["url"])
    
    def save_to_csv(self, output_file: str = None, mode: str = 'w') -> str:
        """
        Sauvegarde les données en CSV.
        
        Args:
            output_file: Chemin du fichier (défaut: config)
            mode: Mode d'écriture ('w' pour overwrite, 'a' pour append)
            
        Returns:
            Chemin du fichier sauvegardé
        """
        output_file = output_file or self.output_file
        output_file = Path(output_file)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        if not self.data:
            logger.warning("Aucune donnée à sauvegarder")
            return str(output_file)
        
        df = pd.DataFrame(self.data, columns=CSV_CONFIG["columns"])
        
        # Supprimer les doublons
        initial_count = len(df)
        df = df.drop_duplicates(subset=["text"], keep='first')
        if initial_count > len(df):
            logger.info(f"Supprimé {initial_count - len(df)} doublons")
        
        try:
            if mode == 'a' and output_file.exists():
                # Append mode: lire le fichier existant, combiner et supprimer les doublons
                existing_df = pd.read_csv(output_file, encoding=CSV_CONFIG["encoding"])
                df = pd.concat([existing_df, df], ignore_index=True)
                df = df.drop_duplicates(subset=["text"], keep='first')
            
            df.to_csv(
                output_file,
                index=CSV_CONFIG["index"],
                encoding=CSV_CONFIG["encoding"]
            )
            logger.info(f"Données sauvegardées: {len(df)} enregistrements dans {output_file}")
            return str(output_file)
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde CSV: {e}")
            raise
    
    def read_csv(self, input_file: str = None) -> pd.DataFrame:
        """
        Lit un fichier CSV.
        
        Args:
            input_file: Chemin du fichier CSV
            
        Returns:
            DataFrame pandas
        """
        input_file = input_file or self.output_file
        input_file = Path(input_file)
        
        if not input_file.exists():
            logger.warning(f"Fichier non trouvé: {input_file}")
            return pd.DataFrame(columns=CSV_CONFIG["columns"])
        
        try:
            df = pd.read_csv(input_file, encoding=CSV_CONFIG["encoding"])
            logger.info(f"Fichier lu: {len(df)} enregistrements")
            return df
        except Exception as e:
            logger.error(f"Erreur lors de la lecture CSV: {e}")
            raise
    
    def get_statistics(self) -> Dict[str, any]:
        """
        Retourne les statistiques des données.
        
        Returns:
            Dictionnaire avec les statistiques
        """
        if not self.data:
            return {
                "total_records": 0,
                "total_urls": 0,
                "unique_urls": 0,
                "avg_text_length": 0
            }
        
        df = pd.DataFrame(self.data)
        return {
            "total_records": len(df),
            "total_urls": len(df),
            "unique_urls": df["url"].nunique(),
            "avg_text_length": df["text"].str.len().mean(),
        }
    
    def clear(self):
        """Vide les données en mémoire."""
        self.data = []
