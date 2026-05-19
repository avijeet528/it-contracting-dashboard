"""Central configuration for extraction pipeline."""
import os
from dataclasses import dataclass, field
from typing import Optional

@dataclass
class Config:
    # Mode
    mode: str = field(default_factory=lambda: os.getenv('EXTRACTION_MODE', 'hybrid'))
    
    # API Keys
    openai_key: Optional[str] = field(default_factory=lambda: os.getenv('OPENAI_API_KEY'))
    llama_key: Optional[str] = field(default_factory=lambda: os.getenv('LLAMA_API_KEY'))
    groq_key: Optional[str] = field(default_factory=lambda: os.getenv('GROQ_API_KEY'))
    github_token: Optional[str] = field(default_factory=lambda: os.getenv('G_TOKEN'))
    
    # Azure / SharePoint
    azure_client_id: Optional[str] = field(default_factory=lambda: os.getenv('AZURE_CLIENT_ID'))
    azure_client_secret: Optional[str] = field(default_factory=lambda: os.getenv('AZURE_CLIENT_SECRET'))
    azure_tenant_id: Optional[str] = field(default_factory=lambda: os.getenv('AZURE_TENANT_ID'))
    sharepoint_site_url: Optional[str] = field(default_factory=lambda: os.getenv('SHAREPOINT_SITE_URL'))
    
    # Models
    openai_model: str = 'gpt-4o-mini'           # Fast + cheap for validation
    openai_model_strong: str = 'gpt-4o'         # For complex inference
    groq_model: str = 'llama-3.3-70b-versatile' # Fast chatbot
    
    # Paths
    quotes_dir: str = field(default_factory=lambda: os.getenv('QUOTES_DIR', '../quotes'))
    output_file: str = '../catalog_data.json'
    cache_dir: str = '.cache'
    
    def has_openai(self) -> bool:
        return bool(self.openai_key)
    def has_llama(self) -> bool:
        return bool(self.llama_key)
    def has_groq(self) -> bool:
        return bool(self.groq_key)
    def has_sharepoint(self) -> bool:
        return all([self.azure_client_id, self.azure_client_secret, self.azure_tenant_id])
    
    def summary(self):
        print('=' * 60)
        print(f'📋 Extraction Configuration')
        print('=' * 60)
        print(f'  Mode:        {self.mode}')
        print(f'  OpenAI:      {"✅" if self.has_openai() else "❌"}')
        print(f'  LlamaParse:  {"✅" if self.has_llama() else "❌"}')
        print(f'  Groq:        {"✅" if self.has_groq() else "❌"}')
        print(f'  SharePoint:  {"✅" if self.has_sharepoint() else "❌"}')
        print(f'  GitHub Token:{"✅" if self.github_token else "❌"}')
        print('=' * 60)

CFG = Config()
