from __future__ import annotations
import html
import json
import re
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
from mrjob.step import MRStep
import logging


URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
TOKEN_RE = re.compile(r"[a-z]+")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Pre-processing functions
def load_stopwords(path: Path) -> set[str]:
    """Load and return a set of lowercase stopwords."""
    with path.open("r", encoding="utf-8") as f:
        return {line.strip().lower() for line in f if line.strip()}


def normalize_text(text: str) -> str:
    """Unescape HTML, remove URLs, and convert to lowercase."""
    text = html.unescape(text)
    text = URL_RE.sub(" ", text)
    return text.lower()


def tokenize(text: str, stopwords: set[str], min_len: int) -> list[str]:
    """Extract alphabetic tokens, skipping short words and stopwords."""
    tokens = TOKEN_RE.findall(text)
    return [tok for tok in tokens if tok not in stopwords and len(tok) >= min_len]


def preprocess_record(record: dict, stopwords: set[str], min_len: int) -> dict:
    """Combine summary and review text, normalize, and tokenize."""
    parts: list[str] = []
    summary = record.get("summary")
    review_text = record.get("reviewText")
    if isinstance(summary, str) and summary.strip():
        parts.append(summary)
    if isinstance(review_text, str) and review_text.strip():
        parts.append(review_text)

    normalized = normalize_text(" ".join(parts))
    tokens = tokenize(normalized, stopwords=stopwords, min_len=min_len)
    return {**record, "clean_tokens": tokens, "clean_text": " ".join(tokens)}


# MapReduce Mrjob implementation

class MapReduce(MRJob):
    """Two-step MapReduce job to collect term frequencies and compute top terms by Chi-Square."""
    OUTPUT_PROTOCOL = RawValueProtocol

    def configure_args(self) -> None:
        super().configure_args()
        self.add_file_arg(
            "--stopwords",
            default="Assignment_1_Assets/stopwords.txt",
            help="Path to stopwords file.",
        )

    def steps(self) -> list[MRStep]:
        return [
            MRStep(mapper_init=self.mapper_init, mapper=self.mapper, combiner=self.combiner, reducer=self.reducer),
            MRStep(
                mapper=self.final_mapper,
                reducer_init=self.final_reducer_init,
                reducer=self.final_reducer,
                jobconf={"mapreduce.job.reduces": "1"},
            ),
        ]

    def mapper_init(self) -> None:
        self._stopwords = load_stopwords(Path(self.options.stopwords))
        self._min_len = 2
        self._top_k = 75

    def mapper(self, _, line: str) -> Iterable[tuple[str, int]]:
        """Parse JSON lines, extract text, tokenize, and yield required document frequencies."""
        line = line.strip()
        if not line:
            return
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            return

        category = record.get("category")
        if not isinstance(category, str) or not category:
            return

        processed = preprocess_record(record, self._stopwords, self._min_len)
        tokens = processed.get("clean_tokens")
        if not isinstance(tokens, list):
            return
        unique_terms = {t for t in tokens if t}

        yield "TOTAL", 1
        yield f"CAT\t{category}", 1
        for term in unique_terms:
            yield f"TERM\t{term}", 1
            yield f"CATTERM\t{category}\t{term}", 1

    def combiner(self, key: str, values: Iterable[int]) -> Iterable[tuple[str, int]]:
        """Combine counts locally before the shuffle phase."""
        yield key, sum(values)

    def reducer(self, key: str, values: Iterable[int]) -> Iterable[tuple[None, str]]:
        """Aggregate frequency counts (total docs, category frequencies, term-category pairs)."""
        count = sum(values)
        yield None, json.dumps({"key": key, "count": count})

    def final_mapper(self, _, line: str) -> Iterable[tuple[None, str]]:
        line = line.strip()
        if line:
            yield None, line

    def final_reducer_init(self) -> None:
        self._total_docs = 0
        self._category_docs: Counter[str] = Counter()
        self._term_docs: Counter[str] = Counter()
        self._cat_term_docs: dict[str, Counter[str]] = defaultdict(Counter)
        self._top_k = 75

    def final_reducer(self, _, lines: Iterable[str]) -> Iterable[tuple[None, str]]:
        """Compute the Chi-Square static for all category-terms, and yield the top K items per category."""
        for line in lines:
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            key = payload.get("key")
            count = payload.get("count")
            if not isinstance(key, str) or not isinstance(count, int):
                continue

            parts = key.split("\t")
            if not parts:
                continue
            tag = parts[0]
            if tag == "TOTAL":
                self._total_docs += count
            elif tag == "CAT" and len(parts) == 2:
                self._category_docs[parts[1]] += count
            elif tag == "TERM" and len(parts) == 2:
                self._term_docs[parts[1]] += count
            elif tag == "CATTERM" and len(parts) == 3:
                category, term = parts[1], parts[2]
                self._cat_term_docs[category][term] += count

        merged_terms: set[str] = set()
        # 1. Compute chi2 for each category-term pair

        for category in sorted(self._category_docs.keys()):
            cat_docs = self._category_docs[category]
            rows: list[tuple[float, str]] = []
            for term, a in self._cat_term_docs[category].items():
                b = cat_docs - a
                c = self._term_docs[term] - a
                d = self._total_docs - (a + b + c)
                denom = (a + c) * (b + d) * (a + b) * (c + d)
                if denom == 0:
                    chi2 = 0.0
                else:
                    chi2 = self._total_docs * (a * d - b * c) ** 2 / denom
                rows.append((chi2, term))
            # 2. sort by chi2 descending and take top K

            rows.sort(key=lambda item: item[0], reverse=True)
            top_items = rows[: self._top_k]

            parts = [category]
            for chi2, term in top_items:
                merged_terms.add(term)
                parts.append(f"{term}:{chi2:.6f}")
            yield None, " ".join(parts)

        # 3. Collect all selected top-K terms across categories and sort them alphabetically
        
        merged_line = " ".join(sorted(merged_terms))
        yield None, merged_line


if __name__ == "__main__":
    started = time.perf_counter()
    MapReduce.run()
    elapsed = time.perf_counter() - started
    hours = int(elapsed // 3600)
    minutes = int((elapsed % 3600) // 60)
    seconds = int(elapsed % 60)
    if hours > 0:
        msg = f"Elapsed: {hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        msg = f"Elapsed: {minutes}m {seconds}s"
    else:
        msg = f"Elapsed: {seconds}s"
    logger.info(msg)
