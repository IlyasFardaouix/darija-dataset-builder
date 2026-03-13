# -*- coding: utf-8 -*-
"""
YouTube Comment Scraper — Collecteur de commentaires Darija depuis YouTube.

Utilise l'API YouTube Data v3 pour extraire massivement les commentaires
des vidéos marocaines populaires (musique, sport, actualités, cuisine, etc.).

Quota API : 10,000 unités/jour (gratuit).
- commentThreads.list = 1 unité par requête (100 commentaires max)
- search.list = 100 unités par requête
- Estimation : ~900k commentaires/jour de quota théorique
"""

import os
import time
import json
import requests
from pathlib import Path
from typing import List, Dict, Generator, Optional, Set
from dotenv import load_dotenv
from src.logger import setup_logger

load_dotenv()
logger = setup_logger(__name__)

# ============================================================================
# CONFIGURATION YOUTUBE
# ============================================================================

YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"
API_KEY = os.getenv("YOUTUBE_API_KEY", "")

# Chaînes YouTube marocaines populaires — couvrent sport, musique, news, cuisine, humour
MOROCCAN_CHANNELS = [
    # === MUSIQUE & DIVERTISSEMENT ===
    "UC2PMGuYGaGMiGSHPv4RlJbQ",   # Saad Lamjarred
    "UCY1kMZp36IQSyNx_9h4mpCg",   # Fnaire
    "UCbNz1j6MhtJfGXsJlTVShig",   # Douzi
    "UC8aG3LDTDwNR1UQhSn9uVrw",   # Hatim Ammor
    "UCEjOF0H_y5v6h-fcJrCqImw",   # Zouhair Bahaoui
    "UCq-Fj5jknLsUf-MWSy4_brA",   # RotanaMusic (vidéos marocaines)
    # === SPORT ===
    "UCp3JD_O13dU7rG8bSxvlAaQ",   # beIN Sports MENA
    "UCCEdmXagMjguJJh2VLK2xRA",   # Arryadia
    # === NEWS & ACTUS ===
    "UCk8ixMxOMXFCnnbAmgGS5Yw",   # Hespress
    "UCBb1BmP0gCp5wuDJJ424G6w",   # 2M Maroc
    "UCSAB-Y4kDz8cFxiJFcbPekA",   # Al Aoula
    "UCW1gG7t5-aSPb1xE-sU79eg",   # Medi1 TV
    "UCR6PjMGDPxSTQi4NfKJkiCA",   # Chouf TV
    "UC6Qg1RNjzYBFdRWVuRY6kbw",   # Le360
    # === HUMOUR & LIFESTYLE ===
    "UCphiYMYD3P7pQCFPaFx0lMA",   # Amine Raghib
    "UC3hxMVBYDZAIi4E30yKnc-Q",   # Mehdi Mozayine
    "UCoY-2raC0HqWgREfFPd3loA",   # Rachid Show
    # === CUISINE MAROCAINE ===
    "UCVAUiCeaFvpzWpvRlHVB-6w",   # Choumicha
    "UCYLclwCrhFQ51BOkA_l2DZg",   # Cuisine Marocaine
]

# Termes de recherche YouTube pour trouver des vidéos avec commentaires Darija
SEARCH_QUERIES_DARIJA = [
    # Sport & Football
    "المنتخب المغربي",
    "الوداد الرجاء الديربي",
    "أسود الأطلس كأس العالم",
    "حكيمي بونو أمرابط",
    "البطولة المغربية",
    "raja wydad match",
    # Musique
    "أغاني مغربية شعبية",
    "راي مغربي جديد",
    "كناوة موسيقى",
    "سعد لمجرد جديد",
    # Cuisine
    "طبخ مغربي تقليدي",
    "طاجين مغربي وصفة",
    "كسكس مغربي",
    "حريرة رمضان",
    # Actualités Maroc
    "أخبار المغرب اليوم",
    "هسبريس جديد",
    "الحياة في المغرب",
    "مغاربة العالم",
    # Humour & Lifestyle
    "كوميديا مغربية مضحكة",
    "فيديوهات مغربية مضحكة",
    "الحياة فالمغرب",
    "darija maroc drole",
    # Culture
    "تاريخ المغرب",
    "مدن المغرب سياحة",
    "العرس المغربي تقاليد",
    "رمضان فالمغرب",
    # Mix FR/AR/Darija
    "maroc vlog darija",
    "marocain reaction",
    "maghreb actualite",
]

# Fichier de sauvegarde JSONL brut
RAW_DATA_DIR = Path(__file__).parent.parent / "data" / "raw"
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
YOUTUBE_RAW_FILE = RAW_DATA_DIR / "youtube_comments.jsonl"
YOUTUBE_PROGRESS_FILE = RAW_DATA_DIR / "youtube_progress.json"


class YouTubeScraper:
    """
    Collecteur de commentaires YouTube marocains via l'API Data v3.
    
    Stratégie de collecte:
    1. Rechercher des vidéos par mots-clés Darija/Maroc
    2. Lister les vidéos des chaînes marocaines populaires
    3. Extraire TOUS les commentaires + réponses de chaque vidéo
    4. Sauvegarder en JSONL brut avec pagination
    5. Respecter le quota API (10,000 unités/jour)
    """
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or API_KEY
        if not self.api_key:
            raise ValueError(
                "YouTube API Key manquante! "
                "Ajoutez YOUTUBE_API_KEY dans le fichier .env"
            )
        self.session = requests.Session()
        self.quota_used = 0
        self.total_comments = 0
        self.processed_videos: Set[str] = set()
        self._load_progress()
    
    def _load_progress(self):
        """Charge la progression précédente pour reprendre là où on s'est arrêté."""
        if YOUTUBE_PROGRESS_FILE.exists():
            try:
                with open(YOUTUBE_PROGRESS_FILE, 'r', encoding='utf-8') as f:
                    progress = json.load(f)
                self.processed_videos = set(progress.get("processed_videos", []))
                self.total_comments = progress.get("total_comments", 0)
                logger.info(
                    f"Progression chargée: {len(self.processed_videos)} vidéos, "
                    f"{self.total_comments} commentaires"
                )
            except Exception as e:
                logger.warning(f"Erreur chargement progression: {e}")
    
    def _save_progress(self):
        """Sauvegarde la progression actuelle."""
        progress = {
            "processed_videos": list(self.processed_videos),
            "total_comments": self.total_comments,
            "quota_used": self.quota_used,
        }
        with open(YOUTUBE_PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(progress, f, ensure_ascii=False, indent=2)
    
    def _api_request(self, endpoint: str, params: dict, cost: int = 1) -> Optional[dict]:
        """
        Fait une requête à l'API YouTube avec gestion d'erreurs et quota.
        
        Args:
            endpoint: Endpoint API (ex: "commentThreads")
            params: Paramètres de la requête
            cost: Coût en unités de quota
            
        Returns:
            Réponse JSON ou None en cas d'erreur
        """
        params["key"] = self.api_key
        url = f"{YOUTUBE_API_BASE}/{endpoint}"
        
        try:
            resp = self.session.get(url, params=params, timeout=30)
            self.quota_used += cost
            
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 403:
                error_reason = resp.json().get("error", {}).get("errors", [{}])[0].get("reason", "")
                if error_reason == "quotaExceeded":
                    logger.warning("⚠️  Quota YouTube dépassé! Attendez demain ou utilisez une autre clé.")
                    return None
                elif error_reason == "commentsDisabled":
                    logger.debug("Commentaires désactivés sur cette vidéo")
                    return None
                else:
                    logger.warning(f"Accès refusé: {error_reason}")
                    return None
            elif resp.status_code == 404:
                logger.debug("Ressource non trouvée")
                return None
            else:
                logger.warning(f"Erreur API {resp.status_code}: {resp.text[:200]}")
                return None
                
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout sur {endpoint}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur réseau: {e}")
            return None
    
    def search_videos(self, query: str, max_results: int = 50) -> List[str]:
        """
        Recherche des vidéos YouTube par mot-clé.
        
        Args:
            query: Terme de recherche
            max_results: Nombre max de vidéos (max 50 par requête)
            
        Returns:
            Liste d'IDs de vidéos
        """
        video_ids = []
        page_token = None
        
        while len(video_ids) < max_results:
            params = {
                "part": "id",
                "q": query,
                "type": "video",
                "maxResults": min(50, max_results - len(video_ids)),
                "regionCode": "MA",
                "relevanceLanguage": "ar",
                "order": "relevance",
            }
            if page_token:
                params["pageToken"] = page_token
            
            data = self._api_request("search", params, cost=100)
            if not data:
                break
            
            for item in data.get("items", []):
                vid_id = item.get("id", {}).get("videoId")
                if vid_id and vid_id not in self.processed_videos:
                    video_ids.append(vid_id)
            
            page_token = data.get("nextPageToken")
            if not page_token:
                break
        
        logger.info(f"Recherche '{query}': {len(video_ids)} vidéos trouvées")
        return video_ids
    
    def get_channel_videos(self, channel_id: str, max_results: int = 200) -> List[str]:
        """
        Récupère les vidéos d'une chaîne YouTube.
        
        Args:
            channel_id: ID de la chaîne
            max_results: Nombre max de vidéos
            
        Returns:
            Liste d'IDs de vidéos
        """
        video_ids = []
        page_token = None
        
        while len(video_ids) < max_results:
            params = {
                "part": "id",
                "channelId": channel_id,
                "type": "video",
                "maxResults": min(50, max_results - len(video_ids)),
                "order": "viewCount",
            }
            if page_token:
                params["pageToken"] = page_token
            
            data = self._api_request("search", params, cost=100)
            if not data:
                break
            
            for item in data.get("items", []):
                vid_id = item.get("id", {}).get("videoId")
                if vid_id and vid_id not in self.processed_videos:
                    video_ids.append(vid_id)
            
            page_token = data.get("nextPageToken")
            if not page_token:
                break
        
        logger.info(f"Chaîne {channel_id}: {len(video_ids)} vidéos")
        return video_ids
    
    def get_video_comments(self, video_id: str, max_comments: int = 0) -> Generator[Dict, None, None]:
        """
        Extrait TOUS les commentaires + réponses d'une vidéo.
        
        Args:
            video_id: ID de la vidéo YouTube
            max_comments: Limite (0 = illimité)
            
        Yields:
            Dict {"text": ..., "url": ..., "source": "youtube", ...}
        """
        page_token = None
        comment_count = 0
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        
        while True:
            params = {
                "part": "snippet,replies",
                "videoId": video_id,
                "maxResults": 100,
                "textFormat": "plainText",
                "order": "relevance",
            }
            if page_token:
                params["pageToken"] = page_token
            
            data = self._api_request("commentThreads", params, cost=1)
            if not data:
                break
            
            for item in data.get("items", []):
                # Commentaire principal
                snippet = item.get("snippet", {}).get("topLevelComment", {}).get("snippet", {})
                text = snippet.get("textDisplay", "").strip()
                if text:
                    yield {
                        "text": text,
                        "url": video_url,
                        "source": "youtube",
                        "author": snippet.get("authorDisplayName", ""),
                        "likes": snippet.get("likeCount", 0),
                        "published": snippet.get("publishedAt", ""),
                    }
                    comment_count += 1
                
                # Réponses au commentaire
                replies = item.get("replies", {}).get("comments", [])
                for reply in replies:
                    reply_snippet = reply.get("snippet", {})
                    reply_text = reply_snippet.get("textDisplay", "").strip()
                    if reply_text:
                        yield {
                            "text": reply_text,
                            "url": video_url,
                            "source": "youtube",
                            "author": reply_snippet.get("authorDisplayName", ""),
                            "likes": reply_snippet.get("likeCount", 0),
                            "published": reply_snippet.get("publishedAt", ""),
                        }
                        comment_count += 1
                
                if max_comments and comment_count >= max_comments:
                    return
            
            page_token = data.get("nextPageToken")
            if not page_token:
                break
        
        self.processed_videos.add(video_id)
        self.total_comments += comment_count
        logger.debug(f"Vidéo {video_id}: {comment_count} commentaires")
    
    def _save_comments_jsonl(self, comments: Generator[Dict, None, None]) -> int:
        """Sauvegarde les commentaires en JSONL (append)."""
        count = 0
        with open(YOUTUBE_RAW_FILE, 'a', encoding='utf-8') as f:
            for comment in comments:
                f.write(json.dumps(comment, ensure_ascii=False) + '\n')
                count += 1
        return count
    
    def scrape_all(self, max_videos_per_query: int = 30,
                   max_videos_per_channel: int = 100,
                   max_comments_per_video: int = 0,
                   save_interval: int = 5) -> int:
        """
        Lance la collecte complète des commentaires YouTube marocains.
        
        Stratégie:
        1. Vidéos des chaînes marocaines populaires
        2. Recherche par mots-clés Darija
        
        Args:
            max_videos_per_query: Vidéos par requête de recherche
            max_videos_per_channel: Vidéos par chaîne
            max_comments_per_video: Limite par vidéo (0=illimité)
            save_interval: Sauvegarder la progression toutes les N vidéos
            
        Returns:
            Nombre total de commentaires collectés
        """
        all_video_ids = []
        
        print(f"\n{'='*60}")
        print(f"  🎬 YOUTUBE SCRAPER — Collecte de commentaires Darija")
        print(f"{'='*60}\n")
        
        # Étape 1: Vidéos des chaînes marocaines
        print(f"📺 Étape 1: Extraction des vidéos de {len(MOROCCAN_CHANNELS)} chaînes marocaines...")
        for channel_id in MOROCCAN_CHANNELS:
            try:
                vids = self.get_channel_videos(channel_id, max_videos_per_channel)
                all_video_ids.extend(vids)
                time.sleep(0.2)
            except Exception as e:
                logger.warning(f"Erreur chaîne {channel_id}: {e}")
        
        print(f"   → {len(all_video_ids)} vidéos depuis les chaînes\n")
        
        # Étape 2: Recherche par mots-clés
        print(f"🔍 Étape 2: Recherche par {len(SEARCH_QUERIES_DARIJA)} mots-clés Darija...")
        for query in SEARCH_QUERIES_DARIJA:
            try:
                vids = self.search_videos(query, max_videos_per_query)
                all_video_ids.extend(vids)
                time.sleep(0.2)
            except Exception as e:
                logger.warning(f"Erreur recherche '{query}': {e}")
        
        # Dédupliquer les IDs
        all_video_ids = list(dict.fromkeys(all_video_ids))
        # Retirer les déjà traités
        all_video_ids = [v for v in all_video_ids if v not in self.processed_videos]
        
        print(f"   → {len(all_video_ids)} vidéos uniques à traiter\n")
        
        if not all_video_ids:
            print("✅ Toutes les vidéos ont déjà été traitées!")
            return self.total_comments
        
        # Étape 3: Extraction des commentaires
        print(f"💬 Étape 3: Extraction des commentaires de {len(all_video_ids)} vidéos...\n")
        
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
                          f"Total: {self.total_comments:,} | "
                          f"Quota: ~{self.quota_used:,} unités")
                
                time.sleep(0.1)  # Respecter le rate limit
                
            except Exception as e:
                logger.warning(f"Erreur vidéo {video_id}: {e}")
                continue
        
        self._save_progress()
        
        print(f"\n{'='*60}")
        print(f"  ✅ YouTube Scraping terminé!")
        print(f"  📊 Commentaires cette session: {session_comments:,}")
        print(f"  📊 Total cumulé: {self.total_comments:,}")
        print(f"  📁 Fichier brut: {YOUTUBE_RAW_FILE}")
        print(f"  💰 Quota utilisé: ~{self.quota_used:,} unités / 10,000")
        print(f"{'='*60}\n")
        
        return session_comments
    
    def read_raw_comments(self) -> Generator[Dict, None, None]:
        """Lit les commentaires bruts depuis le fichier JSONL."""
        if not YOUTUBE_RAW_FILE.exists():
            return
        with open(YOUTUBE_RAW_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        continue
    
    def get_raw_count(self) -> int:
        """Compte le nombre de commentaires bruts collectés."""
        if not YOUTUBE_RAW_FILE.exists():
            return 0
        count = 0
        with open(YOUTUBE_RAW_FILE, 'r', encoding='utf-8') as f:
            for _ in f:
                count += 1
        return count
