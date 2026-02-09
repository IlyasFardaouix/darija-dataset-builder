from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, StaleElementReferenceException,
    WebDriverException, ElementClickInterceptedException
)
from typing import List, Dict, Optional
from urllib.parse import urljoin
from config import FACEBOOK_CONFIG, WEBDRIVER_CONFIG
from src.logger import setup_logger
import time
import random

logger = setup_logger(__name__)


class FacebookScraper:
    """
    Scraper Facebook OPTIMISÉ pour collecter un maximum de commentaires Darija.
    
    Améliorations:
    - Retry automatique avec backoff exponentiel
    - Sélecteurs CSS + XPath combinés (pour robustesse)
    - Clic automatique sur "Voir plus de commentaires" et "Réponses"
    - Scroll intelligent avec détection de fin de page
    - Anti-détection (délais aléatoires, user-agent réaliste)
    - Extraction des sous-commentaires (réponses)
    """
    
    def __init__(self, headless: bool = True):
        """
        Initialise le scraper Facebook.
        
        Args:
            headless: Mode headless (sans interface graphique)
        """
        self.driver = None
        self.headless = headless
        self._init_driver()
    
    def _init_driver(self):
        """Initialise le WebDriver Selenium avec optimisations anti-détection."""
        options = Options()
        
        # Mode headless
        if self.headless:
            options.add_argument('--headless=new')  # Nouveau mode headless Chrome
        
        options.add_argument(f'--window-size={WEBDRIVER_CONFIG["window_size"]}')
        options.add_argument(f'user-agent={WEBDRIVER_CONFIG["user_agent"]}')
        
        # Optimisations de performance
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-plugins')
        options.add_argument('--disable-sync')
        options.add_argument('--disable-default-apps')
        options.add_argument('--disable-background-networking')
        options.add_argument('--disable-blink-features=AutomationControlled')  # Anti-bot
        options.add_argument('--disable-infobars')
        
        # Désactiver les images pour plus de performance
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.default_content_setting_values.notifications": 2,
        }
        options.add_experimental_option("prefs", prefs)
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        
        try:
            self.driver = webdriver.Chrome(options=options)
            self.driver.set_page_load_timeout(FACEBOOK_CONFIG["page_load_timeout"])
            self.driver.implicitly_wait(FACEBOOK_CONFIG["implicit_wait"])
            # Anti-détection: supprimer webdriver flag
            self.driver.execute_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
            logger.info("WebDriver initialisé avec succès (mode anti-détection)")
        except Exception as e:
            logger.error(f"Erreur lors de l'initialisation du WebDriver: {e}")
            raise
    
    def close(self):
        """Ferme le navigateur proprement."""
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
            logger.info("WebDriver fermé")
    
    def _random_delay(self, min_s: float = 0.5, max_s: float = 2.0):
        """Délai aléatoire anti-détection."""
        time.sleep(random.uniform(min_s, max_s))
    
    def scroll_and_load(self, url: str, scroll_times: int = 80, timeout: int = 10, click_more: bool = True):
        """
        Charge une page et effectue un scroll MAXIMAL pour charger tous les commentaires.
        
        Stratégie:
        1. Charger la page
        2. Scroller progressivement en cliquant sur tous les boutons "Voir plus"
        3. Détecter quand on a atteint la fin (pas de nouveau contenu)
        4. Charger les réponses/sous-commentaires
        
        Args:
            url: URL de la publication
            scroll_times: Nombre max de scrolls (80 par défaut = très agressif)
            timeout: Timeout en secondes
            click_more: Cliquer sur tous les boutons "Voir plus"
        """
        try:
            logger.info(f"Chargement: {url}")
            self.driver.get(url)
            time.sleep(3)  # Attendre le chargement initial
            
            last_height = 0
            no_change_count = 0
            
            for i in range(scroll_times):
                # Scroller vers le bas
                self.driver.execute_script("window.scrollBy(0, window.innerHeight);")
                self._random_delay(1.0, 2.0)
                
                # Cliquer sur "Voir plus" à chaque scroll
                if click_more:
                    self._click_all_see_more_buttons()
                    self._click_all_reply_buttons()
                
                # Vérifier si on a atteint la fin
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    no_change_count += 1
                    if no_change_count >= 5:  # 5 scrolls sans changement = fin
                        logger.info(f"Fin de page atteinte après {i+1} scrolls")
                        break
                else:
                    no_change_count = 0
                last_height = new_height
                
                if (i + 1) % 10 == 0:
                    logger.info(f"  Scroll {i+1}/{scroll_times}...")
            
            logger.info(f"Page chargée ({i+1} scrolls effectués)")
            
        except TimeoutException:
            logger.warning(f"Timeout lors du chargement de {url}")
        except Exception as e:
            logger.error(f"Erreur lors du chargement de {url}: {e}")
            raise

    def _click_all_see_more_buttons(self):
        """
        Clique sur TOUS les boutons 'Voir plus de commentaires'.
        Utilise XPath (plus fiable que CSS :contains qui ne fonctionne pas en Selenium).
        """
        xpath_selectors = [
            # Boutons "Voir plus de commentaires" (français)
            '//div[@role="button"][contains(., "plus de commentaires")]',
            '//span[contains(., "plus de commentaires")]/..',
            '//div[@role="button"][contains(., "Afficher plus")]',
            # Boutons "View more comments" (anglais)
            '//div[@role="button"][contains(., "View more comments")]',
            '//span[contains(., "View more comments")]/..',
            # Boutons "Voir plus" génériques
            '//div[@role="button"][contains(., "Voir plus")]',
            # Boutons arabe
            '//div[@role="button"][contains(., "عرض المزيد")]',
            '//div[@role="button"][contains(., "المزيد من التعليقات")]',
        ]
        
        for xpath in xpath_selectors:
            try:
                elements = self.driver.find_elements(By.XPATH, xpath)
                for el in elements:
                    try:
                        if el.is_displayed() and el.is_enabled():
                            self.driver.execute_script("arguments[0].click();", el)
                            self._random_delay(0.3, 0.8)
                    except (StaleElementReferenceException, ElementClickInterceptedException):
                        continue
            except Exception:
                continue
    
    def _click_all_reply_buttons(self):
        """Clique sur les boutons pour afficher les réponses aux commentaires."""
        xpath_selectors = [
            '//div[@role="button"][contains(., "réponse")]',
            '//div[@role="button"][contains(., "réponses")]',
            '//div[@role="button"][contains(., "reply")]',
            '//div[@role="button"][contains(., "replies")]',
            '//span[contains(., "réponse")]/..',
            '//span[contains(., "رد")]/..',
            '//span[contains(., "ردود")]/..',
        ]
        
        for xpath in xpath_selectors:
            try:
                elements = self.driver.find_elements(By.XPATH, xpath)
                for el in elements:
                    try:
                        if el.is_displayed() and el.is_enabled():
                            self.driver.execute_script("arguments[0].click();", el)
                            self._random_delay(0.2, 0.5)
                    except (StaleElementReferenceException, ElementClickInterceptedException):
                        continue
            except Exception:
                continue
    
    def extract_comments(self, post_url: str, max_comments: int = 0, retries: int = 3) -> List[Dict[str, str]]:
        """
        Extrait les commentaires d'une publication avec retry automatique.
        
        Args:
            post_url: URL de la publication
            max_comments: Nombre max de commentaires (0 = illimité)
            retries: Nombre de tentatives
            
        Returns:
            Liste de dictionnaires {text, url}
        """
        for attempt in range(1, retries + 1):
            try:
                return self._extract_comments_impl(post_url, max_comments)
            except Exception as e:
                logger.warning(f"Tentative {attempt}/{retries} échouée pour {post_url}: {e}")
                if attempt < retries:
                    wait_time = 2 ** attempt  # Backoff exponentiel
                    logger.info(f"Attente de {wait_time}s avant retry...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Échec définitif pour {post_url} après {retries} tentatives")
                    return []
    
    def _extract_comments_impl(self, post_url: str, max_comments: int = 0) -> List[Dict[str, str]]:
        """Implémentation interne de l'extraction de commentaires."""
        comments = []
        
        self.scroll_and_load(post_url, scroll_times=80, click_more=True)
        
        # Sélecteurs CSS (les plus fiables actuellement sur Facebook)
        css_selectors = [
            'div[data-testid="UFI2CommentBody_Comment_Text"]',
            'div[dir="auto"][style*="text-align"]',
            'div.xdj266r.x11i5rnm.xat24cr.x1mh8g0r',
            'div[dir="auto"].x11i5rnm',
            'span.x193iq5w.xeuugli',
        ]
        
        # Sélecteurs XPath (backup — plus robustes pour le contenu textuel)
        xpath_selectors = [
            '//div[@data-testid="UFI2CommentBody_Comment_Text"]',
            '//div[contains(@class, "x1lliihq")]//div[@dir="auto"]',
            '//div[@role="article"]//div[@dir="auto"]',
        ]
        
        # Collecter les éléments de commentaires
        comment_elements = []
        
        # D'abord CSS
        for selector in css_selectors:
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    comment_elements.extend(elements)
            except Exception:
                continue
        
        # Puis XPath si pas assez de résultats
        if len(comment_elements) < 10:
            for xpath in xpath_selectors:
                try:
                    elements = self.driver.find_elements(By.XPATH, xpath)
                    if elements:
                        comment_elements.extend(elements)
                except Exception:
                    continue
        
        # Extraire le texte unique
        extracted_texts = set()
        for element in comment_elements:
            try:
                text = element.text.strip()
                if text and len(text) >= 2 and text not in extracted_texts:
                    extracted_texts.add(text)
                    comments.append({
                        "text": text,
                        "url": post_url
                    })
                    if max_comments and len(comments) >= max_comments:
                        break
            except StaleElementReferenceException:
                continue
            except Exception as e:
                logger.debug(f"Erreur extraction commentaire: {e}")
                continue
        
        logger.info(f"✓ {len(comments)} commentaires extraits de {post_url}")
        return comments
    
    def extract_comments_batch(self, post_urls: List[str], delay: float = 2.0) -> List[Dict[str, str]]:
        """
        Extrait les commentaires de plusieurs publications avec délai anti-ban.
        
        Args:
            post_urls: Liste d'URLs
            delay: Délai entre les publications (secondes)
            
        Returns:
            Liste de tous les commentaires extraits
        """
        all_comments = []
        
        for i, url in enumerate(post_urls, 1):
            logger.info(f"[{i}/{len(post_urls)}] Traitement de {url}")
            try:
                comments = self.extract_comments(url)
                all_comments.extend(comments)
                logger.info(f"  → {len(comments)} commentaires (total: {len(all_comments)})")
                
                # Délai anti-ban entre les publications
                if i < len(post_urls):
                    wait = delay + random.uniform(0, 2)
                    time.sleep(wait)
                    
            except Exception as e:
                logger.error(f"Erreur sur {url}: {e}")
                continue
        
        logger.info(f"TOTAL: {len(all_comments)} commentaires depuis {len(post_urls)} publications")
        return all_comments
