# 🥗 FoodRAG  
### Reychel Yakubov  and Maor Ben Yehuda 
### Retrieval-Augmented Generation for Food, Diet & Nutrition

### Video Link : https://drive.google.com/file/d/19HIe8atJbjsTxQ8-P4ujCdqC4-iFViAR/view?usp=sharing

> **FoodRAG** is a fully local Retrieval-Augmented Generation (RAG) system designed to answer food, diet, and nutrition questions by grounding every response in verified textual sources.

The system combines **hybrid retrieval (BM25 + embeddings)**, **cross-encoder re-ranking**, and a **local LLM (via Ollama)** to produce factual, explainable answers without relying on external APIs.

---

## 🧠 System Overview

User Query
│
▼
Query Improvement
(spell check + expansion)
│
▼
Hybrid Retrieval
(BM25 + Embeddings)
│
▼
Vector Store (Chroma)
│
▼
Cross-Encoder Re-Ranking
│
▼
LLM Generation (Ollama)
│
▼
Final Answer + Self-Check



---

## 📂 Supported Data Sources

| Format | Description |
|------|------------|
| `.txt` | Plain nutrition & food documents |
| `.pdf` | Extracted using PyMuPDF (fallback: pdfplumber) |











---

## 🔧 Core Pipeline Components

### 1️⃣ Ingestion & Chunking
- Text extraction
- Cleaning & normalization
- Chunking with overlap
- Metadata enrichment

### 2️⃣ Indexing (ChromaDB)
- Dense embeddings (`sentence-transformers`)
- Persistent local vector store
#### 🔹 Query Understanding & Improvement

Before retrieval, the user query is normalized and expanded to improve recall and robustness.

##### 🔹 Spell Correction
User queries may contain spelling errors or informal wording.  
A lightweight spell-correction step is applied to reduce vocabulary mismatch between the query and indexed documents.

##### 🔹 Synonym Expansion (MeSH-based Vocabulary)
To handle terminology variation in the food and nutrition domain, FoodRAG supports **query expansion using a controlled vocabulary**.

A domain-specific synonym list inspired by **MeSH (Medical Subject Headings)** is used to map common terms to their related concepts.



### 3️⃣ Hybrid Retrieval
Combines lexical and semantic relevance:

FinalScore = α · BM25 + (1 − α) · EmbeddingSimilarity



### 4️⃣ Cross-Encoder Re-Ranking
Improves precision by re-scoring top-K retrieved chunks.

### 5️⃣ Answer Generation (LLM)
- Uses **Ollama** running locally
- Answers strictly based on retrieved context
- Explicitly avoids hallucinations

---


### Models Used

**llama3.2:3b (via Ollama)**
Used for answer generation and verification.
The model generates a draft answer from retrieved documents and then performs a second verification pass to remove unsupported claims and enforce citations.

**Sentence-Transformer Embedding Model (e.g. all-MiniLM-L6-v2)**
Used for semantic retrieval.
Documents and user queries are embedded into vectors to enable similarity search, which is combined with BM25 for hybrid retrieval.

---
### Storage & Vocabulary

**ChromaDB**
Used as the vector database to store document embeddings and enable fast semantic retrieval during query time.

**Medical / Nutrition Vocabulary (MeSH-style synonyms)**
A curated synonym vocabulary is used during query expansion to improve recall for nutrition and health-related terms (e.g., medical terminology, nutrient names, abbreviations).




---

## 🚀 How to Run the Project

### 1️⃣ Requirements

- **Python 3.10+**
- **Ollama installed and running locally** (REQUIRED)

FoodRAG uses a **local LLM via Ollama**.  
Ollama must be installed on your machine before running the notebook.

🔗 Install Ollama:  
https://ollama.com/download

After installation, pull the required model:

```bash
ollama pull llama3.2:3b
```


## 🧪 Running Jupyter Notebooks in PyCharm

### Requirements
- **PyCharm Professional Edition**
- Python interpreter with `jupyter` installed
  
###  Configure Python Interpreter

1. Open **PyCharm → File → Settings**
2. Go to **Project → Python Interpreter**
3. Select a Python **3.10+** interpreter
4. Make sure `jupyter` is installed:
   ```bash
   pip install jupyter ipykernel
   ```



###Git Hygiene: nbstripout (RECOMMENDED)

To keep Git clean and avoid large diffs, notebook outputs are stripped before commit.

Install & Activate

```bash
python -m pip install nbstripout
python -m nbstripout --install
```
Or only for this repository:
```bash
python -m nbstripout --install --attributes .gitattributes
```

This:

Removes outputs

Removes execution counts

Prevents merge conflicts

▶️ Running the Pipeline
Start Ollama

Open the notebook

Run cells top → bottom

Ask questions in the query section

Example:
```bash
"What are good protein sources for a vegetarian diet?"
```

📁 Project Structure
kotlin
Copy code
food-rag-web/
├── notebooks/
│   └── FoodRAG_Project.ipynb
├── data/
│   ├── txt/
│   └── pdf/
├── chroma_food_rag/
├── eval_questions.jsonl
├── vocab/
└── README.md




📝 Notes
The system runs fully locally

No OpenAI / cloud APIs are used

Ollama must be running for generation to work

Chroma vector DB is persisted on disk

