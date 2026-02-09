from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from src.facebook_scraper import FacebookScraper
from src.cleaner import DataCleaner
from src.language_detector import LanguageDetector
from src.csv_manager import CSVManager
from config import PARALLEL_CONFIG, LANGUAGE_CONFIG
from src.logger import setup_logger
import time

logger = setup_logger(__name__)


class DarijaDatasetPipeline:
    """
    Pipeline principal OPTIMISÉ pour créer un dataset ÉNORME de commentaires Darija.
    
    Améliorations:
    - Détection hybride Darija (FastText + heuristiques)
    - Traitement par lots avec progress bar
    - Sauvegarde incrémentale (pas de perte en cas de crash)
    - Statistiques détaillées en temps réel
    - Support pour des centaines de milliers de commentaires
    - Mode append pour accumuler les données
    """
    
    def __init__(self, output_file: str = None, use_scraper: bool = True):
        """
        Initialise le pipeline.
        
        Args:
            output_file: Chemin du fichier CSV de sortie
            use_scraper: Utiliser le scraper Facebook (False = mode commentaires directs)
        """
        self.output_file = output_file
        self.csv_manager = CSVManager(output_file)
        self.cleaner = DataCleaner()
        self.language_detector = LanguageDetector()
        self.scraper = None
        self.processed_count = 0
        self.darija_count = 0
        self.rejected_count = 0
        self.error_count = 0
        self._start_time = time.time()
        
        if use_scraper:
            try:
                self.scraper = FacebookScraper()
            except Exception as e:
                logger.warning(f"Scraper Facebook non disponible: {e}")
                logger.info("Mode sans scraper activé — utilisez process_comments_batch()")
                self.scraper = None
    
    def process_single_post(self, post_url: str) -> int:
        """
        Traite une seule publication Facebook.
        
        Args:
            post_url: URL de la publication
            
        Returns:
            Nombre de commentaires Darija trouvés
        """
        if not self.scraper:
            logger.error("Scraper non initialisé — utilisez process_comments_batch()")
            return 0
        
        try:
            # Extraire les commentaires
            comments = self.scraper.extract_comments(post_url)
            darija_comments = self._process_comments(comments)
            
            self.processed_count += len(comments)
            self.darija_count += len(darija_comments)
            
            return len(darija_comments)
        except Exception as e:
            logger.error(f"Erreur traitement {post_url}: {e}")
            self.error_count += 1
            return 0
    
    def process_multiple_posts(self, post_urls: List[str], batch_size: int = 5,
                                save_interval: int = 10) -> int:
        """
        Traite plusieurs publications avec sauvegarde incrémentale.
        
        Args:
            post_urls: Liste d'URLs
            batch_size: Taille des lots
            save_interval: Sauvegarder tous les N posts
            
        Returns:
            Nombre total de commentaires Darija
        """
        total_darija = 0
        
        with tqdm(total=len(post_urls), desc="📥 Scraping publications") as pbar:
            for i, url in enumerate(post_urls, 1):
                try:
                    darija_count = self.process_single_post(url)
                    total_darija += darija_count
                    pbar.update(1)
                    pbar.set_postfix({
                        "darija": total_darija,
                        "total": self.processed_count,
                        "taux": f"{(self.darija_count/max(self.processed_count,1)*100):.1f}%"
                    })
                    
                    # Sauvegarde incrémentale
                    if i % save_interval == 0:
                        self.save_dataset(mode='w')
                        logger.info(f"💾 Sauvegarde intermédiaire: {total_darija} Darija")
                        
                except Exception as e:
                    logger.error(f"Erreur: {e}")
                    self.error_count += 1
                    pbar.update(1)
        
        return total_darija
    
    def _process_comments(self, comments: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """
        Nettoie et filtre les commentaires pour garder uniquement la Darija.
        
        Pipeline:
        1. Nettoyage (URLs, HTML, normalisation)
        2. Validation longueur
        3. Détection Darija (hybride FastText + heuristiques)
        4. Dédoublonnage
        5. Ajout au CSV
        
        Args:
            comments: Liste de commentaires bruts
            
        Returns:
            Liste de commentaires Darija nettoyés
        """
        darija_comments = []
        
        for comment in comments:
            try:
                text = comment.get("text", "")
                url = comment.get("url", "")
                
                # Étape 1: Nettoyage
                cleaned_text = self.cleaner.clean(text)
                
                # Étape 2: Validité longueur
                if not cleaned_text or not self.cleaner.is_valid_length(cleaned_text):
                    continue
                
                # Étape 3: Détection Darija (hybride — maximise la capture)
                if self.language_detector.is_darija(cleaned_text):
                    darija_comments.append({
                        "text": cleaned_text,
                        "url": url
                    })
                    self.csv_manager.add_record(cleaned_text, url)
                else:
                    self.rejected_count += 1
            
            except Exception as e:
                logger.debug(f"Erreur traitement commentaire: {e}")
                self.error_count += 1
                continue
        
        return darija_comments
    
    def process_comments_batch(self, comments: List[Dict[str, str]]) -> int:
        """
        Traite un lot de commentaires directement (sans scraping).
        
        Args:
            comments: Liste de commentaires [{text, url}, ...]
            
        Returns:
            Nombre de commentaires Darija détectés
        """
        logger.info(f"Traitement de {len(comments)} commentaires...")
        darija_comments = self._process_comments(comments)
        
        self.processed_count += len(comments)
        self.darija_count += len(darija_comments)
        
        logger.info(f"✓ {len(darija_comments)}/{len(comments)} commentaires Darija "
                    f"({len(darija_comments)/max(len(comments),1)*100:.1f}%)")
        
        return len(darija_comments)
    
    def process_comments_streaming(self, comments_iter, batch_size: int = 500,
                                     save_every: int = 2000) -> int:
        """
        Traite des commentaires en mode streaming pour datasets ÉNORMES.
        Sauvegarde périodiquement pour éviter la perte de données.
        
        Args:
            comments_iter: Itérateur de commentaires
            batch_size: Taille des lots de traitement
            save_every: Sauvegarder tous les N commentaires
            
        Returns:
            Nombre total de commentaires Darija
        """
        total_darija = 0
        batch = []
        
        for comment in comments_iter:
            batch.append(comment)
            
            if len(batch) >= batch_size:
                count = self.process_comments_batch(batch)
                total_darija += count
                batch = []
                
                # Sauvegarde périodique
                if self.processed_count % save_every < batch_size:
                    self.save_dataset(mode='w')
                    logger.info(f"💾 Sauvegarde: {total_darija} Darija / {self.processed_count} total")
        
        # Traiter le dernier lot
        if batch:
            count = self.process_comments_batch(batch)
            total_darija += count
        
        return total_darija
    
    def save_dataset(self, output_file: str = None, mode: str = 'w') -> str:
        """
        Sauvegarde le dataset au format CSV.
        
        Args:
            output_file: Chemin du fichier CSV
            mode: Mode d'écriture ('w' = écraser, 'a' = append)
            
        Returns:
            Chemin du fichier sauvegardé
        """
        return self.csv_manager.save_to_csv(output_file, mode)
    
    def get_statistics(self) -> Dict[str, any]:
        """
        Retourne les statistiques complètes du pipeline.
        """
        elapsed = time.time() - self._start_time
        csv_stats = self.csv_manager.get_statistics()
        
        stats = {
            "total_processed": self.processed_count,
            "total_darija": self.darija_count,
            "total_rejected": self.rejected_count,
            "total_errors": self.error_count,
            "darija_percentage": (self.darija_count / max(self.processed_count, 1) * 100),
            "elapsed_seconds": round(elapsed, 2),
            "comments_per_second": round(self.processed_count / max(elapsed, 0.1), 1),
        }
        stats.update(csv_stats)
        return stats
    
    def print_statistics(self):
        """Affiche les statistiques détaillées."""
        stats = self.get_statistics()
        elapsed = stats.get('elapsed_seconds', 0)
        
        print("\n" + "="*60)
        print("📊 STATISTIQUES DU DATASET DARIJA")
        print("="*60)
        print(f"  Total commentaires traités:   {stats.get('total_processed', 0):,}")
        print(f"  Total commentaires Darija:     {stats.get('total_darija', 0):,}")
        print(f"  Commentaires rejetés:          {stats.get('total_rejected', 0):,}")
        print(f"  Erreurs:                       {stats.get('total_errors', 0):,}")
        print(f"  Pourcentage Darija:            {stats.get('darija_percentage', 0):.2f}%")
        print(f"  ---")
        print(f"  Enregistrements CSV:           {stats.get('total_records', 0):,}")
        print(f"  URLs uniques:                  {stats.get('unique_urls', 0):,}")
        print(f"  Longueur moyenne texte:        {stats.get('avg_text_length', 0):.1f} chars")
        print(f"  ---")
        print(f"  Temps écoulé:                  {elapsed:.1f}s")
        print(f"  Vitesse:                       {stats.get('comments_per_second', 0):.0f} comments/s")
        print("="*60 + "\n")
    
    def close(self):
        """Ferme le scraper et sauvegarde."""
        if self.scraper:
            self.scraper.close()
        logger.info("Pipeline fermé")
