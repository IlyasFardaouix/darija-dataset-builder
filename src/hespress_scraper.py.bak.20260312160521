# -*- coding: utf-8 -*-
"""
Hespress Comment Scraper — Collecteur de commentaires Darija depuis Hespress.

Hespress.com est le plus grand site d'actualités au Maroc.
Les commentaires sont massivement en Darija — c'est une mine d'or.

Scraping direct avec requests + BeautifulSoup (pas besoin d'API).
Estimation: ~300k commentaires accessibles.
"""

import time
import json
import re
import requests
from pathlib import Path
from typing import List, Dict, Generator, Optional, Set
from bs4 import BeautifulSoup
from src.logger import setup_logger

logger = setup_logger(__name__)

# ============================================================================
# CONFIGURATION HESPRESS
# ============================================================================

HESPRESS_BASE = "https://www.hespress.com"

# Catégories Hespress avec beaucoup de commentaires Darija
HESPRESS_CATEGORIES = [
    "/sport",
    "/politique",
    "/economie",
    "/societe",
    "/regions",
    "/monde",
    "/culture",
    "/medias",
    "/people",
    "/sante",
    "/tamazight",
    "/sport/football-national",
    "/sport/football-international",
]

# Fichier de sauvegarde JSONL brut
RAW_DATA_DIR = Path(__file__).parent.parent / "data" / "raw"
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
HESPRESS_RAW_FILE = RAW_DATA_DIR / "hespress_comments.jsonl"
HESPRESS_PROGRESS_FILE = RAW_DATA_DIR / "hespress_progress.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ar,fr;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


class HespressScraper:
    """
    Collecteur de commentaires Hespress (site d'actualités marocain).
    
    Stratégie:
    1. Parcourir les catégories pour lister les articles
    2. Pour chaque article, extraire tous les commentaires
    3. Pagination automatique des commentaires
    4. Sauvegarde JSONL incrémentale avec reprise
    """
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.total_comments = 0
        self.processed_urls: Set[str] = set()
        self._load_progress()
    
    def _load_progress(self):
        """Charge la progression précédente."""
        if HESPRESS_PROGRESS_FILE.exists():
            try:
                with open(HESPRESS_PROGRESS_FILE, 'r', encoding='utf-8') as f:
                    progress = json.load(f)
                self.processed_urls = set(progress.get("processed_urls", []))
                self.total_comments = progress.get("total_comments", 0)
                logger.info(
                    f"Hespress: progression chargée — {len(self.processed_urls)} articles, "
                    f"{self.total_comments} commentaires"
                )
            except Exception:
                pass
    
    def _save_progress(self):
        """Sauvegarde la progression."""
        progress = {
            "processed_urls": list(self.processed_urls)[-10000:],  # Garder les 10k derniers
            "total_comments": self.total_comments,
        }
        with open(HESPRESS_PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(progress, f, ensure_ascii=False, indent=2)
    
    def _fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        """Télécharge et parse une page HTML."""
        try:
            resp = self.session.get(url, timeout=20)
            if resp.status_code == 200:
                return BeautifulSoup(resp.content, 'html.parser')
            else:
                logger.debug(f"HTTP {resp.status_code}: {url}")
                return None
        except requests.exceptions.RequestException as e:
            logger.debug(f"Erreur réseau: {e}")
            return None
    
    def get_article_urls(self, category: str, max_pages: int = 50) -> List[str]:
        """
        Récupère les URLs d'articles d'une catégorie Hespress.
        
        Args:
            category: Chemin de la catégorie (ex: "/sport")
            max_pages: Nombre max de pages de listing
            
        Returns:
            Liste d'URLs d'articles
        """
        article_urls = []
        
        for page_num in range(1, max_pages + 1):
            if page_num == 1:
                url = f"{HESPRESS_BASE}{category}"
            else:
                url = f"{HESPRESS_BASE}{category}/page/{page_num}"
            
            soup = self._fetch_page(url)
            if not soup:
                break
            
            # Chercher les liens d'articles
            links = soup.find_all('a', href=True)
            found = 0
            for link in links:
                href = link['href']
                # Les articles Hespress ont un format spécifique
                if (href.startswith(HESPRESS_BASE) and
                    '.html' in href and
                    href not in self.processed_urls and
                    href not in article_urls):
                    article_urls.append(href)
                    found += 1
            
            if found == 0:
                break  # Plus d'articles sur cette page
            
            time.sleep(0.5)  # Politesse
        
        logger.info(f"Hespress {category}: {len(article_urls)} articles")
        return article_urls
    
    def get_article_comments(self, article_url: str) -> Generator[Dict, None, None]:
        """
        Extrait tous les commentaires d'un article Hespress.
        
        Hespress utilise un système de commentaires chargé dynamiquement.
        On tente d'extraire depuis le HTML et via l'API de commentaires Disqus/interne.
        
        Args:
            article_url: URL de l'article
            
        Yields:
            Dict {"text": ..., "url": ..., "source": "hespress"}
        """
        soup = self._fetch_page(article_url)
        if not soup:
            return
        
        # Stratégie 1: Commentaires directement dans le HTML
        comment_selectors = [
            'div.comment-text',
            'div.comment-body',
            'div.comment_text',
            'p.comment-content',
            'div.single-comment-text',
            'div[class*="comment"] p',
            'li[class*="comment"] div.comment-body',
            'div.comments-list div.comment',
        ]
        
        for selector in comment_selectors:
            comments = soup.select(selector)
            for comment_el in comments:
                text = comment_el.get_text(strip=True)
                if text and len(text) >= 3:
                    yield {
                        "text": text,
                        "url": article_url,
                        "source": "hespress",
                    }
        
        # Stratégie 2: API interne de commentaires (AJAX)
        # Hespress charge parfois les commentaires via AJAX
        article_id = self._extract_article_id(article_url, soup)
        if article_id:
            yield from self._fetch_ajax_comments(article_url, article_id)
        
        self.processed_urls.add(article_url)
    
    def _extract_article_id(self, url: str, soup: BeautifulSoup) -> Optional[str]:
        """Extrait l'ID de l'article pour les requêtes AJAX."""
        # Chercher dans les attributs data
        article_el = soup.find(attrs={"data-post-id": True})
        if article_el:
            return article_el["data-post-id"]
        
        # Chercher dans le HTML (wp-post-id pattern)
        body = soup.find('body')
        if body:
            classes = body.get('class', [])
            for cls in classes:
                match = re.search(r'postid-(\d+)', cls)
                if match:
                    return match.group(1)
        
        # Chercher dans les scripts
        scripts = soup.find_all('script')
        for script in scripts:
            text = script.string or ""
            match = re.search(r'"post_id"\s*:\s*"?(\d+)"?', text)
            if match:
                return match.group(1)
        
        return None
    
    def _fetch_ajax_comments(self, article_url: str, article_id: str) -> Generator[Dict, None, None]:
        """Tente de récupérer les commentaires via l'endpoint AJAX WordPress."""
        # WordPress REST API pour les commentaires
        api_url = f"{HESPRESS_BASE}/wp-json/wp/v2/comments"
        page = 1
        
        while True:
            try:
                params = {
                    "post": article_id,
                    "per_page": 100,
                    "page": page,
                    "order": "asc",
                }
                resp = self.session.get(api_url, params=params, timeout=15)
                
                if resp.status_code != 200:
                    break
                
                comments = resp.json()
                if not comments:
                    break
                
                for comment in comments:
                    # Le contenu est en HTML, extraire le texte
                    content_html = comment.get("content", {}).get("rendered", "")
                    text_soup = BeautifulSoup(content_html, 'html.parser')
                    text = text_soup.get_text(strip=True)
                    
                    if text and len(text) >= 3:
                        yield {
                            "text": text,
                            "url": article_url,
                            "source": "hespress",
                            "author": comment.get("author_name", ""),
                            "published": comment.get("date", ""),
                        }
                
                page += 1
                time.sleep(0.3)
                
            except Exception as e:
                logger.debug(f"AJAX commentaires erreur: {e}")
                break
    
    def _save_comments_jsonl(self, comments: Generator[Dict, None, None]) -> int:
        """Sauvegarde en JSONL (append)."""
        count = 0
        with open(HESPRESS_RAW_FILE, 'a', encoding='utf-8') as f:
            for comment in comments:
                f.write(json.dumps(comment, ensure_ascii=False) + '\n')
                count += 1
        return count
    
    def scrape_all(self, max_pages_per_category: int = 50,
                   save_interval: int = 10) -> int:
        """
        Lance la collecte complète de tous les commentaires Hespress.
        
        Args:
            max_pages_per_category: Pages de listing par catégorie
            save_interval: Sauvegarder toutes les N articles
            
        Returns:
            Nombre total de commentaires collectés
        """
        print(f"\n{'='*60}")
        print(f"  📰 HESPRESS SCRAPER — Collecte de commentaires Darija")
        print(f"{'='*60}\n")
        
        # Étape 1: Lister tous les articles
        all_article_urls = []
        print(f"📋 Étape 1: Listing des articles de {len(HESPRESS_CATEGORIES)} catégories...")
        
        for category in HESPRESS_CATEGORIES:
            try:
                urls = self.get_article_urls(category, max_pages_per_category)
                all_article_urls.extend(urls)
                time.sleep(0.5)
            except Exception as e:
                logger.warning(f"Erreur catégorie {category}: {e}")
        
        # Dédupliquer
        all_article_urls = list(dict.fromkeys(all_article_urls))
        all_article_urls = [u for u in all_article_urls if u not in self.processed_urls]
        
        print(f"   → {len(all_article_urls)} articles uniques à traiter\n")
        
        if not all_article_urls:
            print("✅ Tous les articles ont déjà été traités!")
            return self.total_comments
        
        # Étape 2: Extraction des commentaires
        print(f"💬 Étape 2: Extraction des commentaires de {len(all_article_urls)} articles...\n")
        
        session_comments = 0
        for i, url in enumerate(all_article_urls, 1):
            try:
                comments = self.get_article_comments(url)
                count = self._save_comments_jsonl(comments)
                session_comments += count
                self.total_comments += count
                
                if i % save_interval == 0:
                    self._save_progress()
                    print(f"   [{i}/{len(all_article_urls)}] "
                          f"Session: {session_comments:,} | "
                          f"Total: {self.total_comments:,}")
                
                time.sleep(0.3)  # Politesse
                
            except Exception as e:
                logger.warning(f"Erreur article: {e}")
                continue
        
        self._save_progress()
        
        print(f"\n{'='*60}")
        print(f"  ✅ Hespress Scraping terminé!")
        print(f"  📊 Commentaires cette session: {session_comments:,}")
        print(f"  📊 Total cumulé: {self.total_comments:,}")
        print(f"  📁 Fichier brut: {HESPRESS_RAW_FILE}")
        print(f"{'='*60}\n")
        
        return session_comments
    
    def read_raw_comments(self) -> Generator[Dict, None, None]:
        """Lit les commentaires bruts depuis le fichier JSONL."""
        if not HESPRESS_RAW_FILE.exists():
            return
        with open(HESPRESS_RAW_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        continue
    
    def get_raw_count(self) -> int:
        """Compte le nombre de commentaires bruts collectés."""
        if not HESPRESS_RAW_FILE.exists():
            return 0
        count = 0
        with open(HESPRESS_RAW_FILE, 'r', encoding='utf-8') as f:
            for _ in f:
                count += 1
        return count
