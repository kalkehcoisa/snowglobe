"""
Template loader for HTML templates
"""

import os
from pathlib import Path
from typing import Dict, Optional

# Get the directory where templates are stored
TEMPLATE_DIR = Path(__file__).parent / "templates"


class TemplateLoader:
    """
    Simple template loader with caching
    """
    
    def __init__(self, template_dir: Path = TEMPLATE_DIR, cache: bool = True):
        self.template_dir = template_dir
        self.cache = cache
        self._cache: Dict[str, str] = {}
    
    def load(self, template_name: str, variables: Optional[Dict[str, str]] = None) -> str:
        """
        Load a template file and optionally replace variables
        
        Args:
            template_name: Name of template file (e.g., "dashboard.html")
            variables: Dictionary of variables to replace (e.g., {"title": "My Dashboard"})
            
        Returns:
            Template content as string
        """
        # Check cache first
        if self.cache and template_name in self._cache:
            content = self._cache[template_name]
        else:
            # Load from file
            template_path = self.template_dir / template_name
            
            if not template_path.exists():
                raise FileNotFoundError(f"Template not found: {template_name}")
            
            with open(template_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Cache it
            if self.cache:
                self._cache[template_name] = content
        
        # Replace variables if provided
        if variables:
            for key, value in variables.items():
                content = content.replace(f"{{{{{key}}}}}", str(value))
        
        return content
    
    def clear_cache(self):
        """Clear the template cache"""
        self._cache.clear()
    
    def exists(self, template_name: str) -> bool:
        """Check if template exists"""
        return (self.template_dir / template_name).exists()


# Global template loader instance
template_loader = TemplateLoader()


def load_template(template_name: str, variables: Optional[Dict[str, str]] = None) -> str:
    """
    Convenience function to load a template
    
    Args:
        template_name: Name of template file
        variables: Dictionary of variables to replace
        
    Returns:
        Template content as string
    """
    return template_loader.load(template_name, variables)
