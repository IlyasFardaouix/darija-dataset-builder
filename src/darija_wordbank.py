"""
Banque de mots et expressions Darija pour détection heuristique.

La Darija marocaine est un dialecte arabe maghrébin très distinct de l'arabe standard (MSA).
Elle mélange de l'arabe, du français, de l'espagnol et du berbère.
FastText seul ne suffit pas — il faut des heuristiques basées sur le vocabulaire Darija.

Ce module fournit:
- Un dictionnaire exhaustif de mots Darija courants
- Des expressions typiques en Darija
- Des patterns de Darija écrite en caractères latins ("Darija latine" / "3arbiya")
- Une fonction de scoring heuristique pour identifier la Darija
"""

import re
from typing import Set, List, Tuple


# ============================================================================
# MOTS DARIJA COURANTS (écriture arabe)
# Classés par catégorie pour faciliter la maintenance
# ============================================================================

# Salutations et expressions courantes
DARIJA_GREETINGS = {
    "كيداير", "كيدايرة", "كيدايرين",  # Comment ça va (m/f/pl)
    "لاباس", "لا باس",               # Ça va / pas mal
    "بخير", "مزيان", "مزيانة",       # Bien
    "حمدلله", "الحمد لله",           # Dieu merci
    "بسلامة", "تبارك الله",          # Au revoir / Mashallah
    "صباح الخير", "مساء الخير",       # Bonjour / Bonsoir
    "تحياتي",                         # Salutations
    "سلام", "السلام",                 # Salut
    "أهلا", "مرحبا",                  # Bienvenue
    "الله يبارك",                      # Que Dieu bénisse
    "الله يحفظك",                      # Que Dieu te protège
    "الله يعطيك الصحة",                # Que Dieu te donne la santé
    "الله يرحم الوالدين",              # Que Dieu bénisse tes parents
    "شكرا", "بارك الله فيك",          # Merci
}

# Pronoms et mots grammaticaux Darija
DARIJA_GRAMMAR = {
    "ديال", "ديالي", "ديالك", "ديالو", "ديالها", "ديالنا", "ديالكم", "ديالهم",  # Possessifs
    "هاد", "هادي", "هادو", "هداك", "هديك",  # Démonstratifs
    "فين", "فاين", "منين", "كيفاش", "علاش", "شحال", "شنو", "أشنو", "واش",  # Interrogatifs
    "كاين", "كاينة", "كاينين",  # Il y a
    "ماشي", "والو", "حتى حاجة",  # Négation
    "بزاف", "شوية", "قليل",     # Quantité
    "دابا", "دروك", "هادي",      # Maintenant
    "غادي", "غادية", "غاديين",   # Futur (va)
    "كان", "كانت", "كانو",       # Passé (était)
    "كيقول", "كتقول", "كيقولو",  # Dire (présent)
    "كيدير", "كتدير", "كيديرو",  # Faire (présent)
    "عندي", "عندك", "عندو", "عندها", "عندنا",  # Avoir
    "فيه", "فيها", "فيهم",       # Dans
    "معايا", "معاك", "معاه", "معاها", "معانا",  # Avec
    "بحال", "كيف", "كيفما",      # Comme
    "يالله", "يلاه", "أجي",      # Allez / Viens
    "خاص", "خاصني", "خاصك",     # Il faut
    "قبل", "بعد", "مورا",        # Avant/Après
}

# Verbes courants en Darija
DARIJA_VERBS = {
    "مشيت", "مشا", "مشات", "مشاو",       # Aller
    "جيت", "جا", "جات", "جاو",            # Venir
    "كليت", "كلا", "كلات",                 # Manger
    "شربت", "شرب", "شربات",                # Boire
    "شفت", "شاف", "شافت", "شافو",          # Voir
    "سمعت", "سمع", "سمعات",                # Entendre
    "قلت", "قال", "قالت", "قالو",          # Dire
    "درت", "دار", "دارت", "دارو",          # Faire
    "خدمت", "خدم", "خدمات",                # Travailler
    "لقيت", "لقا", "لقات",                 # Trouver
    "بغيت", "بغا", "بغات", "بغاو",         # Vouloir/Aimer
    "عرفت", "عرف", "عرفات",                # Savoir
    "كتبت", "كتب", "كتبات",                # Écrire
    "قريت", "قرا", "قرات",                  # Étudier/Lire
    "نعست", "نعس", "نعسات",                # Dormir
    "فاق", "فاقت", "فاقو",                  # Se réveiller
    "طلع", "طلعت", "طلعو",                  # Monter
    "هبط", "هبطت", "هبطو",                  # Descendre
    "وقف", "وقفت", "وقفو",                  # S'arrêter
    "تسنا", "تسنيت", "تسناو",               # Attendre
    "خلا", "خليت", "خلاو",                  # Laisser
    "عطا", "عطيت", "عطاو", "عطيني",         # Donner
    "خدا", "خديت", "خداو",                  # Prendre
    "ولا", "وليت", "ولاو",                  # Devenir/Retourner
    "بان", "بانت", "بانو", "بان ليا",       # Apparaître/Sembler
    "حط", "حطيت", "حطو",                    # Poser/Mettre
    "طيب", "طيبت", "طيبو",                  # Cuisiner
    "صيفط", "صيفطت", "صيفطو",               # Envoyer
    "تكلم", "تكلمت", "تكلمو",               # Parler
    "ضحك", "ضحكت", "ضحكو",                  # Rire
    "بكا", "بكيت", "بكاو",                  # Pleurer
    "ركب", "ركبت", "ركبو",                  # Monter (véhicule)
}

# Noms et substantifs courants
DARIJA_NOUNS = {
    "خدمة", "خدما",            # Travail
    "دار", "الدار",            # Maison
    "مدرسة", "لقراية",         # École
    "طوموبيل", "طونوبيل",      # Voiture
    "طوبيس", "كار",            # Bus
    "تران", "لقطار",           # Train
    "فلوس", "دراهم", "لفلوس",  # Argent
    "الماكلة", "لماكلة",       # Nourriture
    "لخبز", "خبز",             # Pain
    "أتاي", "اتاي",            # Thé
    "لحليب", "حليب",           # Lait
    "لما", "الماء",             # Eau
    "لحوت", "حوت",             # Poisson
    "لحم", "لحم",              # Viande
    "خضرة", "لخضرة",           # Légumes
    "فاكية", "لفاكية",        # Fruits
    "سوق", "لمارشي",           # Marché
    "حانوت", "لحانوت",         # Boutique
    "مول", "مولات",             # Propriétaire
    "ولد", "بنت", "ولاد",      # Enfants
    "رجل", "مرا", "عيالات",    # Homme/Femme
    "صاحب", "صاحبة", "صحابي",  # Ami(e)
    "لخوي", "خويا", "خوتي",    # Frère (fam.)
    "لالة", "عمتي", "خالتي",   # Tante
    "عمي", "خالي",              # Oncle
    "بابا", "يمّا", "ماما",      # Parents
    "مغرب", "المغرب", "لمغريب", # Maroc
    "كازا", "الرباط", "فاس", "مراكش", "طنجة", "أكادير",  # Villes
    "حومة", "لحومة", "زنقة", "الزنقة",  # Quartier/Rue
    "بلاصة", "لبلاصة",         # Place/Endroit
    "لعب", "الماتش", "كورة",    # Sport/Foot
    "موسيقى", "أغنية", "شعبي",  # Musique
}

# Adjectifs et adverbes
DARIJA_ADJECTIVES = {
    "مليح", "مليحة", "مليحين",      # Bon/Bien
    "خايب", "خايبة", "خايبين",      # Mauvais
    "كبير", "كبيرة", "صغير", "صغيرة",  # Grand/Petit
    "زوين", "زوينة", "زوينين",      # Joli/Beau
    "مسخوط", "مسخوطة",              # Fâché
    "فرحان", "فرحانة", "فرحانين",   # Content
    "حزين", "حزينة",                 # Triste
    "عيان", "عيانة", "مريض", "مريضة",  # Malade
    "ساخن", "بارد",                   # Chaud/Froid
    "بزاف", "شوية",                   # Beaucoup/Peu
    "مزيان", "واعر", "واعرة",        # Super/Génial
    "حسن", "خير",                     # Mieux
    "صعيب", "ساهل",                   # Difficile/Facile
    "غالي", "رخيص",                   # Cher/Pas cher
    "جديد", "قديم",                   # Nouveau/Ancien
    "نقي", "وسخ",                     # Propre/Sale
}

# Expressions et interjections populaires
DARIJA_EXPRESSIONS = {
    "والله", "ولاه", "ولله",        # Je jure
    "بصح", "بالصح",                   # Vraiment
    "سيفطني", "صافي", "سافي",        # C'est fini / assez
    "ياك", "واخا", "واخها",          # D'accord / OK
    "هاكا", "هكا", "هكدا",           # Comme ça
    "عافاك", "عفاك",                  # S'il te plaît
    "يا لطيف", "يا ربي",             # Oh mon Dieu
    "ما عليه", "ماعليه",              # Pas grave
    "عندك الحق", "مساخيط",           # T'as raison
    "حشومة", "عيب",                   # Honte
    "مسكين", "مسكينة",                # Pauvre
    "درياف", "دغيا",                   # Vite
    "شوف", "شوفي",                     # Regarde
    "تبارك الله", "ما شاء الله",       # Mashallah
    "إن شاء الله", "نشالله",           # Inchallah
    "لا حول ولا قوة إلا بالله",        # Expression religieuse
    "أنا معاك", "راه",                 # Je suis avec toi / C'est que
    "هاهو", "هاهي", "هاهم",           # Le/La/Les voilà
    "عاد", "باقي", "مازال",            # Encore
    "خلاص", "سالينا",                  # C'est fini
    "زيد", "زيدي",                     # Continue / En plus
    "بركة", "بركا", "باراكا",          # Assez
    "نتا", "نتي", "نتوما", "حنا", "هما",  # Pronoms personnels
}

# ============================================================================
# DARIJA EN CARACTÈRES LATINS ("Darija latine" / "3arbiya")
# Les chiffres remplacent des sons arabes:
# 3 = ع (ain), 7 = ح (ha), 9 = ق (qaf), 5 = خ (kha), 8 = غ (ghain), 2 = ء (hamza)
# ============================================================================

DARIJA_LATIN = {
    # Salutations
    "salam", "slm", "labas", "la bas", "bikhir", "hamdullah", "hamdlah",
    "bslama", "bsalama", "bonsoir", "sbah lkhir", "msa lkhir",
    "wach", "wsh", "kifash", "ki dayra", "ki dayr", "kidayr", "kidayra",
    "cv", "wa cv",
    
    # Expressions courantes
    "wakha", "wkha", "ok", "iyeh", "la2", "yak", "yaak",
    "bzaf", "bzzaf", "chwiya", "chway", 
    "daba", "drk", "drok", "dork",
    "3lach", "3lash", "fach", "fin", "mnin", "chno", "achno", "chhal",
    "machi", "walo", "walou", "hta haja",
    "3afak", "3fak", "allah y3tik sa7a", "allah ybarek",
    "tbarkallah", "tbark llah", "mashallah", "mchallah",
    "inchallah", "nchallah", "nchalah",
    "wallah", "wlah", "wllah",
    "bsa7", "bssa7",
    "safi", "safi safi", "khalas",
    "7chouma", "3ib", "hchouma",
    "meskine", "mskin", "mskina",
    "dghya", "dghiya",
    "chouf", "choufi",
    "zid", "zidi",
    "barka", "baraka", "braka",
    
    # Mots courants
    "khoya", "khouya", "khouti", "khti", "khtiti",
    "sahbi", "sa7bi", "sa7bti",
    "moul", "mul", "lmoul",
    "dar", "ldar", "lotel",
    "khdma", "lkhdma",
    "flouss", "flous", "drahem",
    "tomobil", "tonobil",
    "tobis", "lkar",
    "atay", "l7lib", "lma", "lkhobz",
    "lma9la", "makla",
    "so9", "lmarchi",
    "7anout", "l7anout",
    "weld", "bent", "wlad", "lbnat",
    "rajel", "mra",
    "baba", "yemma", "mama",
    "lmghrib", "maghrib", "casa", "kaza", "rabat", "fas", "marrakech", "tanja", "agadir",
    "7ouma", "l7ouma", "zan9a",
    "match", "kora",
    
    # Verbes courants
    "mchit", "mcha", "mchat", "mchaw",
    "jit", "ja", "jat", "jaw",
    "klit", "kla", "klat",
    "chrbt", "chrb",
    "chft", "chaf", "chafat", "chafo",
    "sm3t", "sm3", "sm3at",
    "glt", "gal", "galat", "galo",
    "drt", "dar", "darat", "daro",
    "khdmt", "khdm",
    "l9it", "l9a", "l9at",
    "bghit", "bgha", "bghat", "bghaw",
    "3rft", "3rf",
    "ktbt", "ktb",
    "9rit", "9ra", "9rat",
    "n3st", "n3s",
    "ta3", "ta3i", "ta3k", "ta3o", "ta3ha", "ta3na",
    
    # Adjectifs
    "mli7", "mli7a", "zwin", "zwina", "zwnin",
    "khayb", "khayba",
    "fr7an", "fr7ana",
    "7zin", "7zina",
    "3yan", "3yana",
    "wa3r", "wa3ra",
    "s3ib", "sahel",
    "ghali", "rkhis",
    "jdid", "9dim",
}


# ============================================================================
# PATTERNS REGEX POUR DARIJA
# ============================================================================

# Pattern Darija latine: mots avec des chiffres pour sons arabes
DARIJA_LATIN_NUMBER_PATTERN = re.compile(
    r'\b\w*[3279]\w*\b',  # Mots contenant 3, 2, 7, 9 (sons arabes)
    re.IGNORECASE
)

# Pattern écriture arabe
ARABIC_SCRIPT_PATTERN = re.compile(r'[\u0600-\u06FF\u0750-\u077F\uFB50-\uFDFF\uFE70-\uFEFF]')

# Combinaison de tous les mots Darija (arabe)
ALL_DARIJA_ARABIC: Set[str] = set()
ALL_DARIJA_ARABIC.update(DARIJA_GREETINGS)
ALL_DARIJA_ARABIC.update(DARIJA_GRAMMAR)
ALL_DARIJA_ARABIC.update(DARIJA_VERBS)
ALL_DARIJA_ARABIC.update(DARIJA_NOUNS)
ALL_DARIJA_ARABIC.update(DARIJA_ADJECTIVES)
ALL_DARIJA_ARABIC.update(DARIJA_EXPRESSIONS)

# Combinaison de tous les mots Darija (latin)
ALL_DARIJA_LATIN: Set[str] = set()
ALL_DARIJA_LATIN.update(DARIJA_LATIN)


def score_darija(text: str) -> Tuple[float, int]:
    """
    Calcule un score heuristique de "darija-ness" pour un texte.
    
    Stratégie:
    - Chercher des mots Darija connus (arabe et latin)
    - Détecter des patterns spécifiques (chiffres pour sons arabes)
    - Plus le score est élevé, plus c'est probablement du Darija
    
    Args:
        text: Le texte à analyser
        
    Returns:
        Tuple (score normalisé 0-1, nombre de mots Darija trouvés)
    """
    if not text or not isinstance(text, str):
        return (0.0, 0)
    
    text_lower = text.lower().strip()
    darija_word_count = 0
    total_words = 0
    
    # 1. Vérifier les mots Darija en écriture arabe
    # Tokeniser sur les espaces et la ponctuation
    words = re.findall(r'[\u0600-\u06FF]+|[a-zA-Z0-9]+', text)
    total_words = len(words) if words else 1
    
    for word in words:
        # Vérifier dans le dictionnaire arabe
        if word in ALL_DARIJA_ARABIC:
            darija_word_count += 1
        # Vérifier dans le dictionnaire latin
        elif word.lower() in ALL_DARIJA_LATIN:
            darija_word_count += 1
    
    # 2. Vérifier les expressions multi-mots
    for expr in ALL_DARIJA_ARABIC:
        if len(expr) > 3 and expr in text:
            darija_word_count += 1
    
    for expr in ALL_DARIJA_LATIN:
        if len(expr) > 3 and expr in text_lower:
            darija_word_count += 1
    
    # 3. Bonus pour pattern Darija latine (chiffres = sons arabes)
    latin_darija_matches = DARIJA_LATIN_NUMBER_PATTERN.findall(text_lower)
    # Filtrer les vrais nombres
    for match in latin_darija_matches:
        if not match.isdigit() and any(c.isalpha() for c in match):
            darija_word_count += 0.5
    
    # 4. Calculer le score normalisé
    score = min(1.0, darija_word_count / max(total_words * 0.3, 1))
    
    return (score, int(darija_word_count))


def is_darija_heuristic(text: str, min_words: int = 1) -> bool:
    """
    Détermine si un texte est en Darija en utilisant l'heuristique.
    
    Args:
        text: Texte à analyser
        min_words: Nombre minimum de mots Darija requis
        
    Returns:
        True si le texte contient suffisamment de mots Darija
    """
    _, count = score_darija(text)
    return count >= min_words


def has_arabic_script(text: str) -> bool:
    """Vérifie si le texte contient de l'écriture arabe."""
    return bool(ARABIC_SCRIPT_PATTERN.search(text))
