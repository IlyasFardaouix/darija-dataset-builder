# -*- coding: utf-8 -*-
"""
Twitter/X Scraper — Collecteur de commentaires Darija depuis Twitter/X.

Utilise l'API Twitter v2 (Free tier — ~1,500 tweets/mois).
Limité mais utile pour compléter le dataset avec du contenu Twitter marocain.

Le Free tier donne accès au endpoint de recherche récente (7 derniers jours).
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
# CONFIGURATION TWITTER
# ============================================================================

TWITTER_API_BASE = "https://api.twitter.com/2"
BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN", "")

# Requêtes de recherche pour trouver des tweets en Darija
DARIJA_SEARCH_QUERIES = [
    # Darija pure (arabe marocain)
    'الله يبارك فيك lang:ar place_country:MA',
    'واعر بزاف lang:ar',
    'الحمد لله lang:ar place_country:MA',
    'مبروك عليك lang:ar place_country:MA',
    'خاصنا lang:ar place_country:MA',
    # Sports marocains
    'الوداد OR الرجاء OR المنتخب_المغربي lang:ar',
    '#ديما_مغرب lang:ar',
    '#المنتخب_المغربي',
    '#الوداد OR #الرجاء',
    # Hashtags marocains populaires
    '#المغرب lang:ar',
    '#كازا OR #الدارالبيضاء lang:ar',
    '#مغاربة lang:ar',
    '#دارجة lang:ar',
    # Darija latine (Arabizi)
    'wach bzzaf OR labas OR nchalah OR 3afak',
    'darija OR marocain OR maghreb',
    '#maroc darija',
    '#TeamMaroc',
    # Cuisine marocaine
    '#طبخ_مغربي OR #كسكس OR #طاجين',
    '#حريرة OR #رمضان_المغرب',
    # Actualités
    'hespress OR #هسبريس',
    '#أخبار_المغرب',
]

# Fichier de sauvegarde JSONL brut
RAW_DATA_DIR = Path(__file__).parent.parent / "data" / "raw"
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
TWITTER_RAW_FILE = RAW_DATA_DIR / "twitter_comments.jsonl"
TWITTER_PROGRESS_FILE = RAW_DATA_DIR / "twitter_progress.json"


class TwitterScraper:
    """
    Collecteur de tweets Darija via l'API Twitter v2.
    
    Limites du plan Free:
    - 1 App, 1 Project
    - 1,500 tweets/mois en lecture
    - Recherche récente (7 derniers jours seulement)
    - 1 requête/15 secondes
    
    Stratégie:
    - Maximiser chaque requête avec 100 résultats max
    - Rechercher par mots-clés Darija et hashtags marocains
    - Sauvegarder en JSONL avec déduplication
    """
    
    def __init__(self, bearer_token: str = None):
        self.bearer_token = bearer_token or BEARER_TOKEN
        if not self.bearer_token:
            raise ValueError(
                "Twitter Bearer Token manquant! "
                "Ajoutez TWITTER_BEARER_TOKEN dans le fichier .env"
            )
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.bearer_token}",
            "Content-Type": "application/json",
        })
        self.total_tweets = 0
        self.seen_ids: Set[str] = set()
        self.monthly_count = 0  # Compteur mensuel (max 1500)
        self._load_progress()
    
    def _load_progress(self):
        """Charge la progression précédente."""
        if TWITTER_PROGRESS_FILE.exists():
            try:
                with open(TWITTER_PROGRESS_FILE, 'r', encoding='utf-8') as f:
                    progress = json.load(f)
                self.seen_ids = set(progress.get("seen_ids", []))
                self.total_tweets = progress.get("total_tweets", 0)
                self.monthly_count = progress.get("monthly_count", 0)
                logger.info(
                    f"Twitter: progression chargée — {self.total_tweets} tweets "
                    f"({self.monthly_count}/1500 ce mois)"
                )
            except Exception:
                pass
    
    def _save_progress(self):
        """Sauvegarde la progression."""
        progress = {
            "seen_ids": list(self.seen_ids)[-5000:],
            "total_tweets": self.total_tweets,
            "monthly_count": self.monthly_count,
        }
        with open(TWITTER_PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(progress, f, ensure_ascii=False, indent=2)
    
    def search_tweets(self, query: str, max_results: int = 100) -> Generator[Dict, None, None]:
        """
        Recherche des tweets récents via l'API v2.
        
        Args:
            query: Requête de recherche Twitter
            max_results: Résultats par page (max 100 Free tier)
            
        Yields:
            Dict {"text": ..., "url": ..., "source": "twitter"}
        """
        if self.monthly_count >= 1500:
            logger.warning("⚠️  Limite mensuelle Twitter atteinte (1500 tweets)")
            return
        
        url = f"{TWITTER_API_BASE}/tweets/search/recent"
        params = {
            "query": query,
            "max_results": min(max_results, 100),
            "tweet.fields": "created_at,author_id,public_metrics,lang",
        }
        next_token = None
        
        while self.monthly_count < 1500:
            if next_token:
                params["next_token"] = next_token
            
            try:
                resp = self.session.get(url, params=params, timeout=30)
                
                if resp.status_code == 200:
                    data = resp.json()
                    tweets = data.get("data", [])
                    
                    if not tweets:
                        break
                    
                    for tweet in tweets:
                        tweet_id = tweet.get("id", "")
                        if tweet_id in self.seen_ids:
                            continue
                        
                        text = tweet.get("text", "").strip()
                        if text and len(text) >= 3:
                            self.seen_ids.add(tweet_id)
                            self.monthly_count += 1
                            
                            yield {
                                "text": text,
                                "url": f"https://twitter.com/i/status/{tweet_id}",
                                "source": "twitter",
                                "author_id": tweet.get("author_id", ""),
                                "lang": tweet.get("lang", ""),
                                "published": tweet.get("created_at", ""),
                                "likes": tweet.get("public_metrics", {}).get("like_count", 0),
                                "retweets": tweet.get("public_metrics", {}).get("retweet_count", 0),
                            }
                    
                    # Pagination
                    meta = data.get("meta", {})
                    next_token = meta.get("next_token")
                    if not next_token:
                        break
                    
                    # Rate limit: 1 req / 15s pour le Free tier
                    time.sleep(16)
                
                elif resp.status_code == 429:
                    # Rate limited
                    reset_time = int(resp.headers.get("x-rate-limit-reset", time.time() + 900))
                    wait = max(reset_time - time.time(), 60)
                    logger.warning(f"⚠️  Rate limit Twitter. Attente {wait:.0f}s...")
                    time.sleep(wait)
                
                elif resp.status_code == 401:
                    logger.error("❌ Bearer Token Twitter invalide!")
                    return
                
                else:
                    logger.warning(f"Twitter API {resp.status_code}: {resp.text[:200]}")
                    break
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Erreur réseau Twitter: {e}")
                break
    
    def _save_comments_jsonl(self, comments: Generator[Dict, None, None]) -> int:
        """Sauvegarde en JSONL (append)."""
        count = 0
        with open(TWITTER_RAW_FILE, 'a', encoding='utf-8') as f:
            for comment in comments:
                f.write(json.dumps(comment, ensure_ascii=False) + '\n')
                count += 1
        return count
    
    def scrape_all(self, max_per_query: int = 100) -> int:
        """
        Lance la collecte de tweets Darija.
        
        Limité par le Free tier (~1500 tweets/mois).
        
        Returns:
            Nombre de tweets collectés
        """
        print(f"\n{'='*60}")
        print(f"  🐦 TWITTER/X SCRAPER — Collecte de tweets Darija")
        print(f"{'='*60}\n")
        
        if self.monthly_count >= 1500:
            print(f"⚠️  Limite mensuelle atteinte ({self.monthly_count}/1500)")
            print("   Attendez le mois prochain pour continuer.")
            return 0
        
        print(f"📊 Quota mensuel: {self.monthly_count}/1,500 tweets utilisés")
        print(f"🔍 {len(DARIJA_SEARCH_QUERIES)} requêtes de recherche Darija\n")
        
        session_tweets = 0
        
        for i, query in enumerate(DARIJA_SEARCH_QUERIES, 1):
            if self.monthly_count >= 1500:
                print(f"\n⚠️  Limite mensuelle atteinte!")
                break
            
            try:
                print(f"   [{i}/{len(DARIJA_SEARCH_QUERIES)}] '{query[:50]}...'", end=" ")
                tweets = self.search_tweets(query, max_per_query)
                count = self._save_comments_jsonl(tweets)
                session_tweets += count
                self.total_tweets += count
                print(f"→ {count} tweets")
                
                self._save_progress()
                
            except Exception as e:
                logger.warning(f"Erreur requête Twitter: {e}")
                print(f"→ erreur")
                continue
        
        self._save_progress()
        
        print(f"\n{'='*60}")
        print(f"  ✅ Twitter Scraping terminé!")
        print(f"  📊 Tweets cette session: {session_tweets:,}")
        print(f"  📊 Total cumulé: {self.total_tweets:,}")
        print(f"  📊 Quota mensuel: {self.monthly_count}/1,500")
        print(f"  📁 Fichier brut: {TWITTER_RAW_FILE}")
        print(f"{'='*60}\n")
        
        return session_tweets
    
    def read_raw_comments(self) -> Generator[Dict, None, None]:
        """Lit les tweets bruts depuis le fichier JSONL."""
        if not TWITTER_RAW_FILE.exists():
            return
        with open(TWITTER_RAW_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        continue
    
    def get_raw_count(self) -> int:
        """Compte le nombre de tweets bruts."""
        if not TWITTER_RAW_FILE.exists():
            return 0
        count = 0
        with open(TWITTER_RAW_FILE, 'r', encoding='utf-8') as f:
            for _ in f:
                count += 1
        return count
