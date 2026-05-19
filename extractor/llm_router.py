"""
Multi-LLM router with circuit-breaker pattern.
Tracks failures per provider and rotates through:
  Groq (free, fast)   →  Llama (paid)  →  OpenAI (paid)
Once a provider hits its quota or fails 3x, it's marked DEAD for the
rest of the run and we stop wasting calls on it.
"""
import json
import time
import os
from typing import Optional, Dict, List, Callable
from dataclasses import dataclass, field
from config import CFG


@dataclass
class ProviderState:
    name: str
    is_alive: bool = True
    failure_count: int = 0
    quota_exhausted: bool = False
    last_error: str = ''
    success_count: int = 0
    total_calls: int = 0
    
    def mark_failed(self, error: str, is_quota: bool = False):
        self.failure_count += 1
        self.last_error = str(error)[:200]
        if is_quota:
            self.quota_exhausted = True
            self.is_alive = False
            print(f'  💀 {self.name} marked DEAD (quota exhausted)')
        elif self.failure_count >= 3:
            self.is_alive = False
            print(f'  💀 {self.name} marked DEAD (3 consecutive failures)')
    
    def mark_success(self):
        self.success_count += 1
        self.failure_count = 0  # reset on success
    
    def __repr__(self):
        return f'{self.name}(alive={self.is_alive}, ok={self.success_count}, fail={self.failure_count})'


class LLMRouter:
    """Singleton router across the whole pipeline."""
    
    def __init__(self):
        # Order matters: try cheap/fast first
        self.providers: List[ProviderState] = []
        if CFG.has_groq():
            self.providers.append(ProviderState('groq'))
        if CFG.has_llama():
            self.providers.append(ProviderState('llama'))
        if CFG.has_openai():
            self.providers.append(ProviderState('openai'))
        
        print(f'🔀 LLM Router initialized with: {[p.name for p in self.providers]}')
    
    def get_alive_providers(self) -> List[ProviderState]:
        return [p for p in self.providers if p.is_alive]
    
    def all_dead(self) -> bool:
        return len(self.get_alive_providers()) == 0
    
    def call(self, prompt: str, system: str = None, max_tokens: int = 4000,
             temperature: float = 0.1, json_mode: bool = True) -> Optional[str]:
        """
        Try each alive provider in order until one succeeds.
        Returns the response text, or None if all providers are dead.
        """
        if self.all_dead():
            return None
        
        sys_msg = system or 'You return strict JSON only. No markdown, no explanation.'
        
        for provider in self.get_alive_providers():
            provider.total_calls += 1
            try:
                if provider.name == 'groq':
                    text = self._call_groq(prompt, sys_msg, max_tokens, temperature, json_mode)
                elif provider.name == 'openai':
                    text = self._call_openai(prompt, sys_msg, max_tokens, temperature, json_mode)
                elif provider.name == 'llama':
                    text = self._call_llama(prompt, sys_msg, max_tokens, temperature, json_mode)
                else:
                    continue
                
                if text:
                    provider.mark_success()
                    return text
                
            except Exception as e:
                err_str = str(e).lower()
                is_quota = any(k in err_str for k in [
                    'quota', 'insufficient_quota', 'rate_limit', '429',
                    'exceeded', 'billing', 'usage limit'
                ])
                provider.mark_failed(str(e), is_quota=is_quota)
                # Print short error but continue to next provider
                print(f'  ⚠️  {provider.name} failed: {str(e)[:120]}')
                continue
        
        return None
    
    def _call_groq(self, prompt, system, max_tokens, temperature, json_mode):
        from groq import Groq
        client = Groq(api_key=CFG.groq_key, timeout=60.0)
        kwargs = {
            'model': CFG.groq_model,
            'messages': [
                {'role': 'system', 'content': system},
                {'role': 'user', 'content': prompt}
            ],
            'temperature': temperature,
            'max_tokens': max_tokens,
        }
        if json_mode:
            kwargs['response_format'] = {'type': 'json_object'}
        r = client.chat.completions.create(**kwargs)
        return r.choices[0].message.content
    
    def _call_openai(self, prompt, system, max_tokens, temperature, json_mode):
        from openai import OpenAI
        client = OpenAI(api_key=CFG.openai_key, timeout=60.0)
        kwargs = {
            'model': CFG.openai_model,
            'messages': [
                {'role': 'system', 'content': system},
                {'role': 'user', 'content': prompt}
            ],
            'temperature': temperature,
            'max_tokens': max_tokens,
        }
        if json_mode:
            kwargs['response_format'] = {'type': 'json_object'}
        r = client.chat.completions.create(**kwargs)
        return r.choices[0].message.content
    
    def _call_llama(self, prompt, system, max_tokens, temperature, json_mode):
        """Llama API via OpenAI-compatible endpoint.
        If you don't have a Llama chat endpoint, leave this raising and
        it will simply skip Llama in the chain."""
        from openai import OpenAI
        client = OpenAI(
            api_key=CFG.llama_key,
            base_url='https://api.llama-api.com',  # ⬅️ adjust to your endpoint
            timeout=60.0,
        )
        r = client.chat.completions.create(
            model='llama3.3-70b',
            messages=[
                {'role': 'system', 'content': system},
                {'role': 'user', 'content': prompt}
            ],
            temperature=temperature,
            max_tokens=max_tokens,
        )
        return r.choices[0].message.content
    
    def stats(self) -> str:
        lines = ['📊 LLM Router Stats:']
        for p in self.providers:
            status = '✅ alive' if p.is_alive else '💀 dead'
            lines.append(
                f'  {p.name:8} {status}  '
                f'success={p.success_count}  fail={p.failure_count}  '
                f'total={p.total_calls}'
            )
        return '\n'.join(lines)


# Singleton
ROUTER = LLMRouter()
