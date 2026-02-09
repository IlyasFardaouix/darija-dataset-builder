"""
Script d'installation et configuration du projet.
"""

import os
import sys
import subprocess
from pathlib import Path


def check_python_version():
    """Vérifie la version de Python."""
    if sys.version_info < (3, 7):
        print("❌ Python 3.7+ requis")
        sys.exit(1)
    print(f"✓ Python {sys.version.split()[0]}")


def install_dependencies():
    """Installe les dépendances."""
    print("\n📦 Installation des dépendances...")
    
    try:
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", 
            "-r", "requirements.txt", "-q"
        ])
        print("✓ Dépendances installées")
    except subprocess.CalledProcessError:
        print("❌ Erreur lors de l'installation des dépendances")
        sys.exit(1)


def create_directories():
    """Crée les répertoires nécessaires."""
    print("\n📁 Création des répertoires...")
    
    dirs = ["data", "models", "logs", "config"]
    for dir_name in dirs:
        Path(dir_name).mkdir(exist_ok=True)
        print(f"  ✓ {dir_name}/")


def download_fasttext_model():
    """Télécharge le modèle FastText."""
    print("\n🧠 Configuration du modèle FastText...")
    print("  (Le modèle (176 MB) sera téléchargé automatiquement à la première utilisation)")


def verify_installation():
    """Vérifie l'installation."""
    print("\n✅ Vérification de l'installation...")
    
    try:
        import selenium
        print("  ✓ Selenium")
        
        import fasttext
        print("  ✓ FastText")
        
        import pandas
        print("  ✓ Pandas")
        
        import numpy
        print("  ✓ NumPy")
        
        print("\n✅ Installation réussie!")
        return True
    except ImportError as e:
        print(f"  ❌ Erreur: {e}")
        return False


def main():
    """Exécute l'installation complète."""
    print("\n" + "="*60)
    print(" "*10 + "DARIJA DATASET BUILDER - Installation")
    print("="*60)
    
    check_python_version()
    create_directories()
    install_dependencies()
    download_fasttext_model()
    
    if verify_installation():
        print("\n" + "="*60)
        print("🎉 Prêt à utiliser! Exécutez:")
        print("\n  python main.py")
        print("  ou")
        print("  python examples.py")
        print("\n" + "="*60 + "\n")
    else:
        print("\n❌ L'installation n'a pas pu être vérifiée")
        sys.exit(1)


if __name__ == "__main__":
    main()
