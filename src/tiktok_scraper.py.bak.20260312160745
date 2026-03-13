# -*- coding: utf-8 -*-
"""
TikTok Comment Scraper — Collecteur de commentaires Darija depuis TikTok.

Utilise le scraping web (pas d'API officielle nécessaire).
Les vidéos TikTok marocaines génèrent énormément de commentaires Darija.

Estimation: ~250k commentaires accessibles.
"""

import time
import json
import re
import requests
from pathlib import Path
from typing import List, Dict, Generator, Optional, Set
from src.logger import setup_logger

logger = setup_logger(__name__)

# ============================================================================
# CONFIGURATION TIKTOK
# ============================================================================

# Hashtags marocains populaires sur TikTok
MOROCCAN_HASHTAGS = [
    "maroc", "المغرب", "morocco", "marocain",
    "casablanca", "كازا", "rabat", "الرباط",
    "marrakech", "مراكش", "tanger", "طنجة",
    "agadir", "أكادير", "fes", "فاس",
    "darija", "دارجة", "marocaine",
    "cuisinemarocaine", "طبخ_مغربي",
    "footballmaroc", "المنتخب_المغربي",
    "rajaclub", "wydad", "الوداد", "الرجاء",
    "couscous", "كسكس", "tajine", "طاجين",
    "ramadan_maroc", "رمضان_المغرب",
    "bled", "مغربية", "مغربي",
    "tiktokmaroc", "هضرة_مغربية",
    "nokat", "نكت_مغربية", "ضحك_مغربي",
    "mode_marocaine", "عرس_مغربي",
    "الدارجة_المغربية", "maghreb",
]

# Comptes TikTok marocains populaires
MOROCCAN_TIKTOK_USERS = [
    "yassinelkhaldi", "aminux_officiel", "saadlamjarred",
    "choumicha_officiel", "rachidshow", 
    "hmizate.ma", "maroc_buzz", "nktwmaroc",
]

# Fichier de sauvegarde JSONL brut
RAW_DATA_DIR = Path(__file__).parent.parent / "data" / "raw"
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
TIKTOK_RAW_FILE = RAW_DATA_DIR / "tiktok_comments.jsonl"
TIKTOK_PROGRESS_FILE = RAW_DATA_DIR / "tiktok_progress.json"

# Headers pour simuler un navigateur
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ar,fr;q=0.9,en;q=0.8",
    "Referer": "https://www.tiktok.com/",
}


class TikTokScraper:
    """
    Collecteur de commentaires TikTok marocains.
    
    Stratégie:
    1. Rechercher des vidéos par hashtags marocains
    2. Extraire les commentaires via l'API web non-officielle
    3. Sauvegarde JSONL incrémentale
    
    Note: TikTok limite fortement le scraping. Ce module utilise
    des techniques de contournement basiques. Pour un scraping massif,
    considérez l'API Research (accès académique).
    """
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.total_comments = 0
        self.processed_videos: Set[str] = set()
        self._load_progress()
    
    def _load_progress(self):
        """Charge la progression précédente."""
        if TIKTOK_PROGRESS_FILE.exists():
            try:
                with open(TIKTOK_PROGRESS_FILE, 'r', encoding='utf-8') as f:
                    progress = json.load(f)
                self.processed_videos = set(progress.get("processed_videos", []))
                self.total_comments = progress.get("total_comments", 0)
                logger.info(
                    f"TikTok: progression chargée — {len(self.processed_videos)} vidéos, "
                    f"{self.total_comments} commentaires"
                )
            except Exception:
                pass
    
    def _save_progress(self):
        """Sauvegarde la progression."""
        progress = {
            "processed_videos": list(self.processed_videos)[-10000:],
            "total_comments": self.total_comments,
        }
        with open(TIKTOK_PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(progress, f, ensure_ascii=False, indent=2)
    
    def _get_video_ids_from_hashtag(self, hashtag: str, count: int = 50) -> List[str]:
        """
        Tente de récupérer les IDs de vidéos pour un hashtag donné.
        
        Note: TikTok bloque souvent les requêtes automatisées.
        Cette méthode utilise l'endpoint web public.
        """
        video_ids = []
        
        # Méthode 1: API web TikTok (peut être bloquée)
        api_url = f"https://www.tiktok.com/api/challenge/item_list/"
        
        try:
            # D'abord, obtenir l'ID du hashtag
            tag_url = f"https://www.tiktok.com/tag/{hashtag}"
            resp = self.session.get(tag_url, timeout=15, allow_redirects=True)
            
            if resp.status_code == 200:
                # Extraire les IDs de vidéos du HTML/JSON embarqué
                # TikTok embarque les données dans un script SIGI_STATE
                matches = re.findall(r'"id"\s*:\s*"(\d{15,})"', resp.text)
                video_ids = list(set(matches))[:count]
                
                if not video_ids:
                    # Alternative: chercher dans le NEXT_DATA
                    match = re.search(
                        r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
                        resp.text, re.DOTALL
                    )
                    if match:
                        try:
                            data = json.loads(match.group(1))
                            items = (data.get("props", {})
                                        .get("pageProps", {})
                                        .get("itemList", []))
                            for item in items:
                                vid_id = item.get("id", "")
                                if vid_id and vid_id not in self.processed_videos:
                                    video_ids.append(vid_id)
                        except json.JSONDecodeError:
                            pass
            
        except Exception as e:
            logger.debug(f"TikTok hashtag #{hashtag}: {e}")
        
        logger.info(f"TikTok #{hashtag}: {len(video_ids)} vidéos")
        return video_ids
    
    def get_video_comments(self, video_id: str, max_comments: int = 500) -> Generator[Dict, None, None]:
        """
        Extrait les commentaires d'une vidéo TikTok.
        
        Args:
            video_id: ID de la vidéo TikTok
            max_comments: Nombre max de commentaires
            
        Yields:
            Dict {"text": ..., "url": ..., "source": "tiktok"}
        """
        cursor = 0
        count = 0
        video_url = f"https://www.tiktok.com/@user/video/{video_id}"
        
        while count < max_comments:
            api_url = "https://www.tiktok.com/api/comment/list/"
            params = {
                "aweme_id": video_id,
                "count": 50,
                "cursor": cursor,
            }
            
            try:
                resp = self.session.get(api_url, params=params, timeout=15)
                if resp.status_code != 200:
                    break
                
                data = resp.json()
                comments = data.get("comments", [])
                
                if not comments:
                    break
                
                for comment in comments:
                    text = comment.get("text", "").strip()
                    if text and len(text) >= 2:
                        yield {
                            "text": text,
                            "url": video_url,
                            "source": "tiktok",
                            "author": comment.get("user", {}).get("nickname", ""),
                            "likes": comment.get("digg_count", 0),
                        }
                        count += 1
                
                if not data.get("has_more", False):
                    break
                
                cursor = data.get("cursor", cursor + 50)
                time.sleep(0.5)
                
            except Exception as e:
                logger.debug(f"TikTok commentaires erreur: {e}")
                break
        
        if count > 0:
            self.processed_videos.add(video_id)
            self.total_comments += count
            logger.debug(f"TikTok vidéo {video_id}: {count} commentaires")
    
    def _save_comments_jsonl(self, comments: Generator[Dict, None, None]) -> int:
        """Sauvegarde en JSONL (append)."""
        count = 0
        with open(TIKTOK_RAW_FILE, 'a', encoding='utf-8') as f:
            for comment in comments:
                f.write(json.dumps(comment, ensure_ascii=False) + '\n')
                count += 1
        return count
    
    def scrape_all(self, max_videos_per_hashtag: int = 50,
                   max_comments_per_video: int = 500,
                   save_interval: int = 10) -> int:
        """
        Lance la collecte complète des commentaires TikTok marocains.
        
        Returns:
            Nombre total de commentaires collectés
        """
        print(f"\n{'='*60}")
        print(f"  🎵 TIKTOK SCRAPER — Collecte de commentaires Darija")
        print(f"{'='*60}\n")
        
        all_video_ids = []
        
        # Collecter les vidéos par hashtag
        print(f"🏷️  Recherche de vidéos par {len(MOROCCAN_HASHTAGS)} hashtags marocains...")
        for hashtag in MOROCCAN_HASHTAGS:
            try:
                vids = self._get_video_ids_from_hashtag(hashtag, max_videos_per_hashtag)
                all_video_ids.extend(vids)
                time.sleep(1)  # Rate limit TikTok
            except Exception as e:
                logger.debug(f"Hashtag #{hashtag}: {e}")
        
        # Dédupliquer
        all_video_ids = list(dict.fromkeys(all_video_ids))
        all_video_ids = [v for v in all_video_ids if v not in self.processed_videos]
        
        print(f"   → {len(all_video_ids)} vidéos uniques à traiter\n")
        
        if not all_video_ids:
            print("ℹ️  Aucune nouvelle vidéo trouvée.")
            print("   TikTok limite fortement le scraping automatisé.")
            print("   Pour plus de données, utilisez l'API Research.")
            return self.total_comments
        
        # Extraction des commentaires
        print(f"💬 Extraction des commentaires de {len(all_video_ids)} vidéos...\n")
        
        session_comments = 0
        for i, video_id in enumerate(all_video_ids, 1):
            try:
                comments = self.get_video_comments(video_id, max_comments_per_video)
                count = self._save_comments_jsonl(comments)
                session_comments += count
                
                if i % save_interval == 0:
                    self._save_progress()
                    print(f"   [{i}/{len(all_video_ids)}] "
                          f"Session: {session_comments:,} | "
                          f"Total: {self.total_comments:,}")
                
                time.sleep(0.5)
                
            except Exception as e:
                logger.debug(f"TikTok vidéo {video_id}: {e}")
                continue
        
        self._save_progress()
        
        print(f"\n{'='*60}")
        print(f"  ✅ TikTok Scraping terminé!")
        print(f"  📊 Commentaires cette session: {session_comments:,}")
        print(f"  📊 Total cumulé: {self.total_comments:,}")
        print(f"  📁 Fichier brut: {TIKTOK_RAW_FILE}")
        print(f"{'='*60}\n")
        
        return session_comments
    
    def read_raw_comments(self) -> Generator[Dict, None, None]:
        """Lit les commentaires bruts depuis le fichier JSONL."""
        if not TIKTOK_RAW_FILE.exists():
            return
        with open(TIKTOK_RAW_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        continue
    
    def get_raw_count(self) -> int:
        """Compte le nombre de commentaires bruts."""
        if not TIKTOK_RAW_FILE.exists():
            return 0
        count = 0
        with open(TIKTOK_RAW_FILE, 'r', encoding='utf-8') as f:
            for _ in f:
                count += 1
        return count
