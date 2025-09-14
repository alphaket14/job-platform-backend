# AI-Powered Job Data Ingestion & Synchronization Pipeline

## 1. Overview

This project is a high-performance, asynchronous ETL pipeline designed to ingest raw job data from XML feeds, enrich it using Google's Gemini AI, and synchronize the results with an external API (Xano). The system is built on a modern Python stack featuring FastAPI, `asyncpg`, and is fully containerized with Docker for easy deployment and scalability.

The pipeline's core function is to transform raw job listings into a structured, valuable dataset by classifying them into industry verticals, generating standardized metadata, and scoring their relevance. It uses a local PostgreSQL database as a "source of truth" to perform robust, sequential synchronization with external services.

---

## 2. Architecture & Workflow

The pipeline operates in a series of sequential stages, ensuring data integrity and accuracy at each step.

                                      [START]
                                         │
                                         ▼
                           ┌─────────────────────────────┐      ┌───────────────────┐
                           │ 1. Ingest & Pre-filter      │      │ Store Closed jobs │ 
                           │ (Stream XML, Remove CLOSED) │─────►│   (Rejected)      │   
                           └────────────┬────────────────┘      └───────────────────┘
                                        │
                                        ▼
                           ┌───────────────────────────┐
                           │ 2. Deduplicate & Aggregate│
                           │ (Hash, Multi-Location)    │
                           └────────────┬──────────────┘
                                        │
                                        ▼
                           ┌───────────────────────────┐      ┌───────────────────┐
                           │ 3. AI Enrichment (Stage 1)│      │ Confidence< 0.5   │       
                           │ (Industry, Confidence)    │─────►│   (archived_jobs) │
                           └────────────┬──────────────┘      └───────────────────┘
                                        │
                                        ▼
                           ┌───────────────────────────┐
                           │ 4. AI Enrichment (Stage 2)│
                           │ (Job Function)            │
                           └────────────┬──────────────┘
                                        │
                                        ▼
              ┌──────────────────────────────────────────────────┐
              │ 5. Categorize Jobs (Based on AI Results)         │
              └────────────────┬────────────────┬────────────────┘
                               │                │
         ┌─────────────────────▼───────────┐    │
         │ AI Process FAILED               │    │
         │ (e.g., API error, timeout)      │    │
         └─────────────────────────────────┘    │
                     │                          │
                     │                          ▼
                     │                 ┌──────────────────────────────┐
                     │                 │ AI Process SUCCEEDED         │
                     │                 │ (Confidence Score is valid)  │
                     │                 └───────────┬──────────────────┘
                     │                             │
                     │                             ▼
                     │                  ┌─────────────────────────────┐
                     │                  │ 6. Route by Confidence Score│
                     │                  └──────┬──────────┬───────────┘
                     │                         │          │
                     │               ┌─────────▼──────┐ ┌─▼───────────────┐
                     │               │ Confidence<0.86│ │ Confidence>0.86 │
                     │               │ (Manual)       │ │ (Approved)      │
                     │               └─────────┬──────┘ └─┬───────────────┘
                     │                         │          │
                     ▼                         ▼          ▼
           ┌──────────────────────────────────────────────────────────────┐
           │ 7. Save ALL Categories to Supabase (Source of Truth)         │
           │     (open_jobs, manual_review_jobs, rejected_jobs)           │
           └───────────────────────────┬──────────────────────────────────┘
                                       │
                                       ▼
           ┌──────────────────────────────────────────────────────────────┐
           │ 8. Full Synchronization with Xano (for Approved Jobs Only)   │
           │    a. Get current jobs from Xano                             │
           │    b. Delete jobs from Xano that are not in Supabase         │
           │    c. Add jobs to Xano that are in Supabase but not Xano     │
           └───────────────────────────┬──────────────────────────────────┘
                                       │
                                       ▼
                                     [END]

---

## 3. Core Features

- **Asynchronous from the Ground Up**: Built with `asyncio`, FastAPI, and `asyncpg` for high-performance, non-blocking I/O.
- **Streaming XML Parser**: Efficiently processes large XML feeds from URLs without loading the entire file into memory.
- **Intelligent AI Enrichment**:
    - A multi-stage process using Google's Gemini model to generate standardized titles, descriptions, skills, and industry classifications.
    - **Correct Context Handling**: Groups jobs by industry before classifying functions to ensure the AI always has the correct set of options.
- **Robust Error Handling**: Automatically retries failed API calls (for both AI and external services) with exponential backoff and gracefully handles malformed data.
- **Dual Execution Modes**: Can be run as a command-line script for single feeds or as a persistent FastAPI server for API-driven execution.
- **Full External Synchronization**: Implements a sequential "delete-then-add" workflow to ensure an external service (Xano) is a perfect mirror of the Supabase database.
- **Containerized Deployment**: Includes a `Dockerfile` and `docker-compose.yml` for easy, consistent deployment in any environment.

---

## 4. Multi-Location Aggregation

A key feature of the pipeline is its ability to identify and merge job postings that are for the same role but are listed in different locations. This prevents duplicates and creates a cleaner dataset. The process works in two passes:

### Pass 1: Before AI Enrichment

This initial pass groups jobs based on their original, raw data.

1.  **Key Generation**: A unique key is created for each job using its `title`, `company_name`, and `employment_type`.
2.  **Grouping**: All jobs with the exact same key are placed into a group.
3.  **Similarity Analysis**: Within each group, the job descriptions are compared using fuzzy string matching to determine if they are for the same role.
4.  **Merging**: Jobs that are determined to be duplicates are merged into a single record. The locations from all the duplicate jobs are combined into one list, and the `is_multi_location` flag is set to `true`.

### Pass 2: After AI Enrichment

After the first stage of AI enrichment, a second, more accurate aggregation pass is performed. This pass uses the standardized `ai_title` generated by the Gemini model, which is cleaner and more consistent than the original title, leading to better matching.

---

## 5. Project Setup

### Prerequisites

- Python 3.11+
- Docker & Docker Compose
- Access to a PostgreSQL database (e.g., Supabase, Neon, or a local instance)

### Step-by-Step Installation

1.  **Clone the Repository**
    ```bash
    git clone <your-repository-url>
    cd <your-repository-name>
    ```

2.  **Create a Virtual Environment**
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # On Windows, use: .venv\Scripts\activate
    ```

3.  **Install Dependencies**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure Environment Variables**
    -   Copy the sample `.env.sample` file to a new file named `.env`.
        ```bash
        cp .env.sample .env
        ```
    -   Open the `.env` file and fill in your actual credentials for the database, Google Cloud, and Xano.

---

## 6. Running the Application

### Method 1: Command-Line Interface (for a single feed)

This method is ideal for testing, debugging, or manually processing a specific feed.

-   **Usage:**
    ```bash
    python process_feed.py --feed-id <ID_OF_THE_FEED>
    ```
-   **Example:**
    ```bash
    python process_feed.py --feed-id 21
    ```

### Method 2: API Server (for continuous operation)

This method runs the application as a high-performance web server using FastAPI and is the recommended approach for production.

1.  **Start the Server:**
    ```bash
    uvicorn app:app --host 0.0.0.0 --port 8080
    ```

2.  **Trigger a Feed:**
    Send a `POST` request to the `/trigger-feed/{feed_id}` endpoint.
    -   **Example using `curl`:**
        ```bash
        curl -X POST http://localhost:8080/trigger-feed/21
        ```
    -   **Response:** The server will immediately respond with a `202 Accepted` status, confirming that the task has started in the background.

3.  **Check Execution Status:**
    Send a `GET` request to the `/execution-status/{feed_id}` endpoint to see the report from the last run.
    -   **Example using `curl`:**
        ```bash
        curl http://localhost:8080/execution-status/21
        ```

4.  **API Documentation:**
    While the server is running, you can access interactive API documentation (Swagger UI) by navigating to `http://localhost:8080/docs` in your browser.

---

## 7. Docker Deployment

1.  **Ensure your `.env` file is complete.**
2.  **Build and Run the Container:**
    ```bash
    docker-compose up --build
    ```
    The application will be accessible at `http://localhost:8080`.

---

## 8. Monitoring & Logging

- **Execution Reports**: The `execution_reports` table in the database stores detailed statistics for each pipeline run.
- **Feed Statistics**: The `feeds` table is updated after every run with a summary, including `last_run_status`, `avg_processing_time`, and `consecutive_failure_count`.
- **Log Levels**: The application provides detailed logs at INFO, WARNING, and ERROR levels to track the progress and diagnose issues.
