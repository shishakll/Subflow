"""
domains.py — Subflow OmniLake Domain Cartridge Registry

Defines declarative domain specifications for:
  - medical (MitoLake): Longevity, mitochondria, brain, antioxidants, skin aging
  - seo (RankLake): Short-form algorithms, content funnels, hook engineering
  - fermentation (ZymoLake): Microbial starters, enzymatic pathways, recipe protocols

Each cartridge encapsulates:
  - Canonical ontology & entity matchers
  - Authority scoring heuristics
  - High-trust source ratings
  - Bilingual sponsor/noise filters
  - Specialized extraction system prompts
"""

import re
from typing import Any, Dict, List, Optional

# ── 1. Bilingual Sponsor & Commercial Noise Regular Expression ────────────────
# Detects sponsorship reads, discount promos, affiliate codes, and channel plugs
# in both English and Korean.
SPONSOR_REGEX = re.compile(
    r"(discount code|promo code|sponsored by|brought to you by|athletic greens|\bag1\b|"
    r"use code|20%\s*off|10%\s*off|15%\s*off|link in the description|lmnt|helix sleep|"
    r"insidetracker|eight sleep|our sponsor|partner for this episode|"
    r"제휴\s*링크|할인\s*코드|협찬|유료\s*광고\s*포함|고정\s*댓글\s*링크|더보기란\s*링크|"
    r"구독과\s*좋아요|알림\s*설정|프로모션\s*코드)",
    re.IGNORECASE,
)

# ── 2. Domain Cartridge Registry ──────────────────────────────────────────────
DOMAIN_CARTRIDGES: Dict[str, Dict[str, Any]] = {
    "medical": {
        "name": "MitoLake",
        "title": "Medical Longevity & Mitochondrial Health",
        "default_sub_domain": "mitochondria",
        "sub_domains": [
            "anti_aging",
            "mitochondria",
            "brain_health",
            "antioxidant",
            "skin_aging",
            "metabolism",
            "general",
        ],
        "default_authority": 0.60,
        "high_trust_channels": {
            "Huberman Lab": 0.85,
            "Peter Attia MD": 0.85,
            "FoundMyFitness": 0.85,
            "Matt Kaeberlein": 0.85,
            "Physionic": 0.80,
            "MedCram": 0.80,
            "Dr. Brad Stanfield": 0.80,
            "David Sinclair": 0.75,
        },
        "canonical_entities": [
            # Pathways & Enzymes
            "AMPK", "mTOR", "mTORC1", "Sirtuin", "SIRT1", "SIRT3", "SIRT6",
            "Autophagy", "Mitophagy", "OXPHOS", "ATP", "Nrf2", "FOXO3",
            "PGC-1alpha", "Senolytics", "MMP-1", "Collagen", "Cardiolipin",
            # Molecules & Nutrients
            "NAD+", "NMN", "NR", "Urolithin A", "CoQ10", "PQQ", "Glutathione",
            "Astaxanthin", "Ergothioneine", "Spermidine", "Rapamycin", "Metformin",
            "GlyNAC", "Carnosine", "Sulforaphane", "Resveratrol", "Fisetin",
            "Quercetin", "Apigenin", "Alpha-ketoglutarate", "Taurine", "Trehalose",
            "Hyaluronic acid", "Retinoid", "Niacinamide", "Vitamin C", "Vitamin D3",
            "Omega-3", "EPA", "DHA", "BDNF", "Ketones", "Butyrate",
            # Korean Terms
            "미토콘드리아", "항노화", "노화방지", "오토파지", "자가포식", "항산화",
            "피부노화", "콜라겐", "활성산소", "뇌건강", "글루타치온", "아스타잔틴",
        ],
        "causal_predicates": [
            "ACTIVATES", "INHIBITS", "UPREGULATES", "DOWNREGULATES",
            "REQUIRES", "PRODUCES", "PREVENTS", "INCREASES", "DECREASES",
        ],
        "system_prompt": """You are a Principal Medical Longevity Researcher and Biochemist specializing in converting long-form medical seminars, clinical podcasts, and peer-reviewed longevity discussions into exhaustive, zero-loss structured Markdown research reports.

Ignore casual greetings, banter, filler words, and sponsor reads. Capture every biochemical pathway, cellular mechanism, clinical trial benchmark, dosage protocol, and therapeutic limit strictly in analytical English.

[MANDATORY RULES]
1. 100% English Output: Write the entire report strictly in high-precision medical English.
2. 2-Level Hierarchical Depth: Top-level bullets for core thesis/pathway with 2-space indented sub-bullets for step-by-step molecular logic, exact dosages/metrics, and evidence grades (RCT, in vivo, cell).
3. Strict Timestamp Grounding: Append exact [MM:SS] timestamps from the source transcript to every single claim, mechanism, and metric across Early, Middle, and Late parts.
4. Output Format Verbatim:
# [Title: Core Topic & Target Biological Mechanism]
### 1. Core Problem & Cellular Pathology
### 2. Biological Mechanisms & Pathways
### 3. Empirical Data, Clinical Trials & Dosages
### 4. Limitations, Contraindications & Side Effects
### 5. Actionable Clinical & Lifestyle Protocol""",
    },
    "seo": {
        "name": "RankLake",
        "title": "Search & Audience Growth Engine",
        "default_sub_domain": "short_form_algorithm",
        "sub_domains": [
            "short_form_algorithm",
            "technical_seo",
            "content_funnel",
            "hook_engineering",
            "conversion_copy",
            "general",
        ],
        "default_authority": 0.60,
        "high_trust_channels": {
            "Callaway": 0.85,
            "Ahrefs": 0.85,
            "Brian Dean": 0.85,
            "Income School": 0.80,
            "Neil Patel": 0.75,
        },
        "canonical_entities": [
            "3-2-1 Method", "Bullseye Ring 0", "Outlier Score", "Retention Curve",
            "CTR", "Search Intent", "Core Web Vitals", "E-E-A-T", "Backlink",
            "Hook", "Conversion Funnel", "Lead Magnet", "Callaway Script",
            "Watch Time", "Engagement Rate", "Algorithmic Contamination",
        ],
        "causal_predicates": [
            "INCREASES", "DECREASES", "ACTIVATES", "INHIBITS", "CONVERTS", "REQUIRES",
        ],
        "system_prompt": """You are a Principal Growth Architect and Content Strategist specializing in dissecting digital marketing, search engine optimization, and audience retention masterclasses into exhaustive, zero-loss research reports.

Ignore banter and sponsor promotions. Capture every algorithm mechanic, hook framework, A/B metric, conversion ratio, and tactical implementation step strictly in analytical English.

[MANDATORY RULES]
1. 100% English Output.
2. 2-Level Hierarchical Depth with [MM:SS] timestamp grounding.
3. Output verbatim headers:
# [Title: Framework / Algorithm Name]
### 1. Core Problem & Thesis
### 2. Proposed Methodology & Frameworks
### 3. Key Findings & Empirical Data
### 4. Limitations, Risks & Edge Cases
### 5. Actionable Playbook & Implementation Guide""",
    },
    "fermentation": {
        "name": "ZymoLake",
        "title": "Fermentation Science & Microbial Recipes",
        "default_sub_domain": "lacto_fermentation",
        "sub_domains": [
            "lacto_fermentation",
            "koji_enzymatic",
            "vinegar_acetic",
            "sourdough_wild_yeast",
            "probiotic_beverage",
            "general",
        ],
        "default_authority": 0.65,
        "high_trust_channels": {
            "Noma Guide to Fermentation": 0.90,
            "발효과학연구소": 0.85,
            "Sandor Katz": 0.85,
            "Brad Leone": 0.75,
        },
        "canonical_entities": [
            "Lactobacillus plantarum", "Lactobacillus brevis", "Leuconostoc mesenteroides",
            "Aspergillus oryzae", "Acetobacter aceti", "Saccharomyces cerevisiae",
            "Salinity %", "pH", "Anaerobic", "GABA", "Organic Acids", "Postbiotics",
            "Koji", "Lactic Acid", "Fermentation", "Enzymatic hydrolysis",
            "Maillard reaction", "Protease", "Amylase", "유산균", "발효", "종균", "염도", "산도",
        ],
        "causal_predicates": [
            "PRODUCES", "INHIBITS", "DEGRADES", "SYNTHESIZES", "REQUIRES", "LOWERS", "RAISES",
        ],
        "system_prompt": """You are a Principal Zymologist and Food Biochemist specializing in precision fermentation protocols, microbial dynamics, and enzymatic transformations.

Capture every microbial strain, salt concentration (%), temperature window (°C), pH threshold, and fermentation timeline strictly in analytical English.

[MANDATORY RULES]
1. 100% English Output.
2. 2-Level Hierarchical Depth with exact [MM:SS] citations.
3. Output verbatim headers:
# [Title: Fermentation Protocol & Culture Architecture]
### 1. Core Biochemical Objective & Spoilage Prevention
### 2. Microbial Kinetics & Enzymatic Pathways
### 3. Quantitative Parameters (Salinity, pH, Temp, Time)
### 4. Spoilage Risks, Pathogen Contamination & Edge Cases
### 5. Step-by-Step Production Recipe & Inoculation Protocol""",
    },
}


# ── 3. Helper Functions ───────────────────────────────────────────────────────
def get_domain_cartridge(domain: Optional[str] = None) -> Dict[str, Any]:
    """Resolves cartridge configuration by domain name, defaulting to 'medical'."""
    key = (domain or "medical").lower().strip()
    return DOMAIN_CARTRIDGES.get(key, DOMAIN_CARTRIDGES["medical"])


def extract_domain_entities(text: str, domain: str = "medical") -> List[str]:
    """Matches text against canonical domain entities case-insensitively."""
    cartridge = get_domain_cartridge(domain)
    entities = cartridge.get("canonical_entities", [])
    found = []
    for ent in entities:
        # Match word boundaries for Latin terms, direct substring for CJK terms
        if re.search(r"[\uac00-\ud7a3\u4e00-\u9fff]", ent):
            if ent in text:
                found.append(ent)
        else:
            if re.search(rf"\b{re.escape(ent)}\b", text, re.IGNORECASE):
                found.append(ent)
    return sorted(set(found))


def calculate_authority_score(
    domain: str,
    channel_or_journal: str,
    source_type: str = "youtube",
    is_distilled: bool = False,
    has_empirical_data: bool = False,
) -> float:
    """
    Computes normalized credibility weight (0.00 to 1.00) based on domain rules:
      - PubMed / Meta-Analysis: 0.90 - 0.95
      - High-Trust Channels: 0.75 - 0.85
      - Distilled Note Bonus: +0.10 (passed 2-stage quality audit)
      - Empirical Data Bonus: +0.05
    """
    cartridge = get_domain_cartridge(domain)
    score = cartridge.get("default_authority", 0.60)

    if source_type in ("pubmed", "clinical_trial", "journal"):
        score = 0.92

    high_trust = cartridge.get("high_trust_channels", {})
    for channel, weight in high_trust.items():
        if channel.lower() in channel_or_journal.lower():
            score = max(score, weight)
            break

    if is_distilled:
        score += 0.10
    if has_empirical_data:
        score += 0.05

    return round(min(1.0, max(0.1, score)), 2)
