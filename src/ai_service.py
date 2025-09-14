"""
AI service for job enrichment using Google's Gemini model via the Vertex AI SDK.
This version uses a robust, concurrent-safe, multi-region architecture.
"""
import itertools
import os
import json
import asyncio
import logging
import time
import re
import hashlib
from typing import Dict, Any, List
from collections import defaultdict

import google.auth
import google.auth.exceptions
import vertexai
from vertexai.generative_models import (
    GenerativeModel,
    Tool,
    ToolConfig,
    FunctionDeclaration,
    HarmCategory,
    HarmBlockThreshold,
)
from google.cloud.aiplatform import initializer as aiplatform_initializer
from google.api_core import exceptions as api_core_exceptions
from google.cloud import storage

from filelock import FileLock, Timeout
import httpx

from .config import Config
from .ai_schemas import (
    UNIFIED_INITIAL_ENRICHMENT_SCHEMA,
    JOB_FUNCTION_SELECTION_SCHEMA,
    SEARCHABLE_TEXT_SCHEMA
)

logger = logging.getLogger(__name__)
storage_client = storage.Client()

def clean_html(raw_html: str) -> str:
    """Removes HTML tags and normalizes whitespace."""
    if not isinstance(raw_html, str):
        return ""
    cleantext = re.sub(re.compile("<.*?>"), "", raw_html)
    return re.sub(r"\s+", " ", cleantext).strip()


class AIService:
    _model_pool: Dict[str, GenerativeModel] = {}
    _init_lock = asyncio.Lock()

    CACHE_FILE = "industry_hierarchy_cache.json"
    CACHE_TTL = 60 * 60 * 24 * 7
    UNIFIED_ENRICHMENT_CACHE_FILE = "unified_enrichment_cache.json"
    JOB_FUNCTIONS_CACHE_FILE = "job_functions_cache.json"
    JOB_FUNCTION_SELECTION_CACHE_FILE = "job_function_selection_cache.json"
    SEARCHABLE_TEXT_CACHE_FILE = "searchable_text_cache.json"


    def __init__(self, statistics=None):
        self.statistics = statistics
        self.bucket = None
        if Config.GCS_CACHE_BUCKET:
            self.bucket = storage_client.bucket(Config.GCS_CACHE_BUCKET)

    @classmethod
    def _sync_get_model_for_location(cls, location: str, system_instruction: List[str]) -> GenerativeModel:
        """Synchronous helper for model initialization to be run in a thread."""
        if location not in cls._model_pool:
            logger.info(f"Initializing new GenerativeModel for location: {location}")
            original_location = aiplatform_initializer.global_config.location
            vertexai.init(project=Config.GOOGLE_PROJECT_ID, location=location)
            cls._model_pool[location] = GenerativeModel(Config.GEMINI_MODEL, system_instruction=system_instruction)
            if original_location != location:
                vertexai.init(project=Config.GOOGLE_PROJECT_ID, location=original_location)
        model = cls._model_pool[location]
        model._system_instruction = system_instruction
        return model

    @classmethod
    async def _get_model_for_location(cls, location: str, system_instruction: List[str]) -> GenerativeModel:
        """Safely initializes and retrieves a model by running blocking code in a thread."""
        async with cls._init_lock:
            return await asyncio.to_thread(
                cls._sync_get_model_for_location, location, system_instruction
            )

    def _sync_load_cache(self, filename: str) -> Dict:

        if not self.bucket: return {}
        try:
            blob = self.bucket.blob(filename)
            logger.info("DEBUG: Checking if blob exists in GCS...")

            if blob.exists():
                logger.info("DEBUG: Blob exists, downloading...")

                json_data = blob.download_as_text()
                return json.loads(json_data)
        except Exception as e:
            logger.warning(f"Could not load GCS cache file {filename}: {e}")
        return {}

    async def _load_cache(self, filename: str) -> Dict:
        return await asyncio.to_thread(self._sync_load_cache, filename)

    def _sync_save_cache(self, filename: str, cache: Dict):
        if not self.bucket: return
        try:
            blob = self.bucket.blob(filename)
            json_data = json.dumps(cache, indent=2)
            blob.upload_from_string(json_data, content_type='application/json')
        except Exception as e:
            logger.error(f"Error saving cache to GCS {filename}: {e}")

    async def _save_cache(self, filename: str, cache: Dict):
        await asyncio.to_thread(self._sync_save_cache, filename, cache)

    @staticmethod
    def _parse_gemini_response(response) -> List[Dict[str, Any]]:
        results = []
        if not response or not hasattr(response, 'candidates'): return results
        for candidate in response.candidates:
            if candidate.finish_reason.name != "STOP":
                logger.warning(f"Candidate finished with reason: {candidate.finish_reason.name}.")
                continue
            for part in candidate.content.parts:
                if part.function_call and (args := part.function_call.args):
                    if isinstance(args, dict) and (items := args.get(list(args.keys())[0])):
                        if isinstance(items, list):
                            results.extend(item for item in items if isinstance(item, dict))
        return results

    async def _process_gemini_batch_with_retries(self, uncached_jobs: List[Dict[str, Any]], location: str,
                                                 prompt_builder, tool, cache, cache_key_builder,
                                                 batch_num: int = 0, total_batches: int = 0, prompt_context=None):
        if not uncached_jobs: return []
        log_prefix = f"Sending batch {batch_num}/{total_batches}"
        max_retries = 3
        delay = 2
        safety_settings = {
            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
        }
        for attempt in range(max_retries):
            try:
                logger.info(
                    f"Attempt {attempt + 1}/{max_retries} for batch {batch_num}/{total_batches}: Building prompt and sending to Gemini in '{location}'...")
                system_prompt, user_prompt = await asyncio.to_thread(
                    prompt_builder, uncached_jobs, prompt_context
                )
                model = await self._get_model_for_location(location, [system_prompt])
                tool_config = ToolConfig(
                    function_calling_config=ToolConfig.FunctionCallingConfig(
                        mode=ToolConfig.FunctionCallingConfig.Mode.ANY
                    )
                )
                if self.statistics: self.statistics.increment_api_call("gemini")
                response = await asyncio.wait_for(
                    model.generate_content_async(
                        contents=[user_prompt], tools=[tool], tool_config=tool_config,
                        safety_settings=safety_settings
                    ), timeout=Config.GEMINI_API_TIMEOUT)

                logger.info(
                    f"Received response for batch {batch_num}/{total_batches} from location {location}. Parsing results...")
                if self.statistics and hasattr(response, 'usage_metadata'):
                    self.statistics.add_gemini_tokens(response.usage_metadata.prompt_token_count,
                                                      response.usage_metadata.candidates_token_count)

                gemini_results = self._parse_gemini_response(response)
                logger.info(
                    f"Successfully parsed {len(gemini_results)} results from AI for batch {batch_num}/{total_batches}.")

                batch_results = []
                if len(gemini_results) != len(uncached_jobs):
                    logger.error(
                        f"Batch {batch_num} failed: AI returned {len(gemini_results)} jobs, but {len(uncached_jobs)} were sent.")
                    for job in uncached_jobs:
                        job['ai_enrichment_status'] = 'failed'
                        job['ai_enrichment_error'] = 'AI response length mismatch'
                        batch_results.append(job)
                    return batch_results  # Return early

                # Use zip to safely iterate and update by index
                for original_job, ai_result in zip(uncached_jobs, gemini_results):
                    original_job.update(ai_result)
                    original_job['ai_enrichment_status'] = 'success'
                    cache[cache_key_builder(original_job)] = ai_result
                    batch_results.append(original_job)
                return batch_results
            except api_core_exceptions.InvalidArgument as e:
                logger.warning(
                    f"Batch {batch_num}/{total_batches} failed due to InvalidArgument (often token limit). Splitting batch.")
                if len(uncached_jobs) <= 1:
                    uncached_jobs[0].update({'ai_enrichment_status': 'failed',
                                             'ai_enrichment_error': 'Single job exceeds model token limit'})
                    return uncached_jobs
                mid = len(uncached_jobs) // 2
                task1 = self._process_gemini_batch_with_retries(uncached_jobs[:mid], location, prompt_builder, tool,
                                                                cache, cache_key_builder, prompt_context=prompt_context)
                task2 = self._process_gemini_batch_with_retries(uncached_jobs[mid:], location, prompt_builder, tool,
                                                                cache, cache_key_builder, prompt_context=prompt_context)
                results = await asyncio.gather(task1, task2)
                return results[0] + results[1]
            except (asyncio.TimeoutError, api_core_exceptions.ResourceExhausted) as e:
                logger.warning(f"Gemini call timed out or resource exhausted on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(delay)
                    delay *= 2
                else:
                    raise e
            except Exception as e:
                logger.error(f"Unexpected error in Gemini batch for location {location} on attempt {attempt + 1}: {e}",
                             exc_info=True)
                if attempt < max_retries - 1:
                    await asyncio.sleep(delay)
                    delay *= 2
                else:
                    raise e
        for job in uncached_jobs: job.update(
            {'ai_enrichment_status': 'failed', 'ai_enrichment_error': f'Batch failed after {max_retries} retries.'})
        return uncached_jobs


    async def _execute_batches_concurrently(self, all_uncached_jobs, prompt_builder, tool, cache, cache_key_builder,
                                            location_cycler, prompt_context=None):
        batch_size = Config.AI_ENRICHMENT_BATCH_SIZE
        semaphore = asyncio.Semaphore(Config.MAX_CONCURRENT_AI_CALLS)
        total_batches = (len(all_uncached_jobs) + batch_size - 1) // batch_size
        async def worker(batch, batch_num):
            async with semaphore:
                location = next(location_cycler)
                return await self._process_gemini_batch_with_retries(batch, location, prompt_builder, tool,
                                                                    cache, cache_key_builder, batch_num,
                                                                    total_batches, prompt_context)
        tasks = [worker(all_uncached_jobs[i:i + batch_size], i // batch_size + 1) for i in
                 range(0, len(all_uncached_jobs), batch_size)]
        processed_batches = await asyncio.gather(*tasks, return_exceptions=True)
        results = []
        for i, batch_result in enumerate(processed_batches):
            if isinstance(batch_result, Exception):
                start_index = i * batch_size
                failed_jobs = all_uncached_jobs[start_index: start_index + batch_size]
                error_message = f'Batch processing failed with exception: {str(batch_result)[:100]}'
                logger.error(f"Batch {i+1}/{total_batches} failed entirely. Error: {batch_result}", exc_info=isinstance(batch_result, Exception))
                for job in failed_jobs: job.update(
                    {'ai_enrichment_status': 'failed', 'ai_enrichment_error': error_message})
                results.extend(failed_jobs)
            else:
                results.extend(batch_result)
        return results

    async def get_initial_enrichment_with_gemini(self, jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        tool = Tool(function_declarations=[
            FunctionDeclaration(name="enrich_jobs", description="Generates enriched data for jobs.",
                                parameters=UNIFIED_INITIAL_ENRICHMENT_SCHEMA)])
        logger.info("AI_SERVICE_DEBUG: Attempting to get industry hierarchy...")
        industry_hierarchy = await self.get_industry_hierarchy_async()
        logger.info("AI_SERVICE_DEBUG: Successfully retrieved industry hierarchy.")
        if not industry_hierarchy:
            for job in jobs: job.update(
                {'ai_enrichment_status': 'failed', 'ai_enrichment_error': 'Failed to fetch industry hierarchy'})
            return jobs
        logger.info("AI_SERVICE_DEBUG: Attempting to load cache from GCS...")
        cache = await self._load_cache(self.UNIFIED_ENRICHMENT_CACHE_FILE)
        logger.info("AI_SERVICE_DEBUG: Successfully loaded cache.")
        results, uncached_jobs = [], []
        for job in jobs:
            cache_key = job.get('job_hash')
            if cache_key in cache:
                job.update(cache[cache_key])
                job['ai_enrichment_status'] = 'success'
                results.append(job)
                if self.statistics: self.statistics.increment_cache_hit("initial_enrichment")
            else:
                uncached_jobs.append(job)
        if uncached_jobs:
            location_cycler = itertools.cycle(Config.GOOGLE_LOCATIONS)
            processed_results = await self._execute_batches_concurrently(
                uncached_jobs,
                self.build_unified_initial_enrichment_prompt,
                tool,
                cache,
                lambda j: j.get('job_hash'),
                location_cycler,
                prompt_context=industry_hierarchy
            )
            results.extend(processed_results)
        await self._save_cache(self.UNIFIED_ENRICHMENT_CACHE_FILE, cache)
        return results

    async def _process_industry_group(self, industry_id: int, jobs_in_group: List[Dict[str, Any]], tool, cache,
                                      location_cycler):
        results, uncached_jobs = [], []
        for job in jobs_in_group:
            cache_key = job.get('job_hash')
            if cache_key and cache_key in cache:
                job.update(cache[cache_key])
                job['ai_enrichment_status'] = 'success'
                results.append(job)
                if self.statistics: self.statistics.increment_cache_hit("job_function_selection")
            else:
                uncached_jobs.append(job)
        if uncached_jobs:
            processed_results = await self._execute_batches_concurrently(
                uncached_jobs, self.build_job_function_batch_prompt, tool, cache,
                lambda j: j.get('job_hash'),
                location_cycler
            )
            results.extend(processed_results)
        return results

    async def classify_job_functions_with_gemini(self, jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        tool = Tool(function_declarations=[
            FunctionDeclaration(name="select_job_functions", description="Selects job functions for jobs.",
                                parameters=JOB_FUNCTION_SELECTION_SCHEMA)])
        cache = await self._load_cache(self.JOB_FUNCTION_SELECTION_CACHE_FILE)
        industry_groups = defaultdict(list)
        skipped_jobs = []
        for job in jobs:
            if job.get('job_functions') and job.get('industry_id'):
                industry_groups[job.get('industry_id')].append(job)
            else:
                job.update({'ai_enrichment_status': 'failed',
                            'ai_enrichment_error': 'Missing data for function classification'})
                skipped_jobs.append(job)

        location_cycler = itertools.cycle(Config.GOOGLE_LOCATIONS)
        tasks = [
            self._process_industry_group(industry_id, jobs_in_group, tool, cache, location_cycler)
            for industry_id, jobs_in_group in industry_groups.items()
        ]
        results_from_groups = await asyncio.gather(*tasks)

        all_processed_jobs = [job for group_result in results_from_groups for job in group_result]
        all_processed_jobs.extend(skipped_jobs)
        await self._save_cache(self.JOB_FUNCTION_SELECTION_CACHE_FILE, cache)
        return all_processed_jobs

    async def get_industry_hierarchy_async(self):
        if self.bucket:
            cached_data = await self._load_cache(self.CACHE_FILE)
            if cached_data:
                if self.statistics: self.statistics.increment_cache_hit("industry_hierarchy")
                return cached_data
        if self.statistics: self.statistics.increment_api_call("industry_hierarchy")
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                resp = await client.get("https://api.collabwork.com/api:app_a4P496M6/industry/sector-industry")
            resp.raise_for_status()
            data = resp.json()
            await self._save_cache(self.CACHE_FILE, data)
            return data
        except httpx.HTTPStatusError as e:
            # Log detailed HTTP errors (like 403 Forbidden, 404 Not Found, 500 Server Error)
            logger.error(f"HTTP error fetching industry hierarchy: {e.response.status_code} - {e.response.text}")
        except httpx.RequestError as e:
            # Log general network errors (like DNS issues, connection refused)
            logger.error(f"Network error fetching industry hierarchy: {e}")
        except json.JSONDecodeError as e:
            # Log errors if the API returns invalid JSON
            logger.error(f"Failed to parse industry hierarchy JSON response: {e}")
        except Exception as e:
            # Catch any other unexpected errors
            logger.error(f"An unexpected error occurred while fetching industry hierarchy: {e}", exc_info=True)
        return None

    async def get_job_functions_for_industry_async(self, industry_id: int):
        if self.statistics: self.statistics.increment_api_call("job_functions")
        params = {'industry_id': industry_id}

        max_retries = 3
        delay = 2
        for attempt in range(max_retries):
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    resp = await client.get("https://api.collabwork.com/api:app_a4P496M6/industry/full-hierarchy-list",
                                            params=params)
                resp.raise_for_status()  # Will raise an exception for 4xx/5xx errors
                return resp.json()
            except httpx.RequestError as e:
                logger.warning(
                    f"Network error fetching job functions for industry {industry_id} on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(delay)
                else:
                    return []  # Return empty list after final failed attempt
        return []

    async def get_job_functions_batch_async(self, jobs: List[Dict[str, Any]]) -> Dict[int, List[Dict[str, Any]]]:
        if not jobs: return {}

        cache = await self._load_cache(self.JOB_FUNCTIONS_CACHE_FILE)
        functions_by_industry_id = {}
        ids_to_fetch = set()

        unique_industry_ids = {job['industry_id'] for job in jobs if job.get('industry_id')}
        for industry_id in unique_industry_ids:
            cache_key = str(industry_id)
            if cache_key in cache and (time.time() - cache[cache_key].get('timestamp', 0)) < self.CACHE_TTL:
                functions_by_industry_id[industry_id] = cache[cache_key]['functions']
                if self.statistics: self.statistics.increment_cache_hit("job_functions")
            else:
                ids_to_fetch.add(industry_id)

        if ids_to_fetch:
            logger.info(f"Found {len(ids_to_fetch)} unique industry IDs to fetch job functions for from API.")
            semaphore = asyncio.Semaphore(15)

            async def worker(industry_id):
                async with semaphore:
                    return industry_id, await self.get_job_functions_for_industry_async(industry_id)

            tasks = [worker(industry_id) for industry_id in ids_to_fetch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if not isinstance(result, Exception) and result:
                    industry_id, functions = result
                    functions_by_industry_id[industry_id] = functions
                    cache[str(industry_id)] = {'timestamp': time.time(), 'functions': functions}

            await self._save_cache(self.JOB_FUNCTIONS_CACHE_FILE, cache)


        for job in jobs:
            if industry_id := job.get('industry_id'):
                job['job_functions'] = functions_by_industry_id.get(industry_id, [])

        return functions_by_industry_id

    @classmethod
    def build_unified_initial_enrichment_prompt(cls, jobs, industry_hierarchy):
        system_prompt = (
            "You are an expert AI data enrichment engine. Your task is to analyze each job posting provided and return a single, comprehensive JSON object containing structured attributes, a precise industry classification, and a confidence score based on the rules below."
            "\n\n"
            "--- TASK 1: GENERATE STRUCTURED JOB ATTRIBUTES ---\n"
            "Generate standardized, professional attributes for each job.\n"
            "**REQUIRED ATTRIBUTE FIELDS:**\n"
            "- ai_title: Improved, standardized job title (professional, clear — exclude location and salary).\n"
            "- ai_description: A brief and professional job description (3–5 well-structured sentences).\n"
            "- ai_job_tasks: List of 3-4 key job responsibilities.\n"
            "- ai_search_terms: List of 4–6 highly relevant search keywords that someone would use to find this job.\n"
            "- ai_top_tags:  List of 3–4 critical technical or functional tags most relevant to the job\n"
            "- ai_skills: List of 4–6 specific skills required for the role.\n"
            "- salary_min: numeric value or null (extract minimum salary if range given)\n"
            "- salary_max: numeric value or null (extract maximum salary if range given)\n"
            "- salary_period: \"Hourly\",\"Weekly\", \"Monthly\", \"Yearly\", or null\n"
            "- currency: \"USD\", \"EUR\", etc. or null\n\n"
            "SALARY EXTRACTION RULES:\n"
            "- **IMPORTANT** Extract salary if explicitly mentioned in the job title or description\n"
            "- Return null for any salary field not found in title or description\n"
            "- Convert amounts to numeric values (remove $, commas, k/m/b suffixes)\n"
            "- If range given (e.g., $50k-$80k), set both min and max\n"
            "- If single value, set both min and max to same value\n"
            "- Standardize periods: Hourly/Weekly/Monthly/Yearly\n\n"
            "\n\n"
            "--- TASK 2: CLASSIFY THE JOB'S INDUSTRY ---\n"
            "Using the provided 'Industry Hierarchy' JSON, classify the company's industry.\n"
            "1. Analyze Job Details: Carefully read the job **Title**, **Company** and **Description** to understand the company's primary business and the role's context.\n"
            "2.  **Follow Hierarchy Logic:** Classify top-down: first Sector, then Industry Group, then Industry.\n"
            "3.  **Best-Fit Principle:** The classification must reflect the *company's* industry, not just the job function (e.g., a 'Software Engineer' at a bank is in the 'Financials' sector).\n"
            "**REQUIRED CLASSIFICATION FIELDS:**\n"
            "- sector_id: (integer)\n"
            "- industry_group_id: (integer)\n"
            "- industry_id: (integer)\n"
            "- reasoning_industry: A short 2-3 line summary of your classification decision."
            "\n\n"
            "--- TASK 3: CALCULATE A CONFIDENCE SCORE ---\n"
            "Evaluate if the job is relevant to one of Morning Brew's newsletters and produce a confidence score from 1-100.\n"
            "**Criterion	Weight	Description**"
            "1. Audience & Role Alignment	85%	This is the critical factor. Does the job directly align with the target audience of one or more of the specific newsletters listed below? A job outside these domains will score extremely low.\n"
            "2. Tone and Voice	6%	Is the language sharp and conversational?\n"
            "3. Clarity and Structure	4%	Is the post easy to skim?\n"
            "4. Impact-Oriented Language	3%	Does the description focus on mission and impact?\n"
            "5. Engaging Hook	2%	Does the post have a compelling opening?\n"

            "** TARGET NEWSLETTERS FOR ALIGNMENT**\n"
            "Your evaluation for 'Audience & Role Alignment' must map the job to one of the following newsletters. If it does not fit any, it fails this criterion.And set the confidence to very low.\n"
            "• Healthcare Brew: For professionals in health administration, health tech, pharma, biotech, medical devices, and clinical leadership (e.g., Nurse Manager, Nurse Informaticist, Nurse Practitioner, Clinical Research Nurse).\n"
            "• Tech Brew: For professionals in software development, product management, data science, venture capital, and high-level tech strategy.\n"
            "• IT Brew: For hands-on technology professionals in cybersecurity, cloud engineering, DevOps, and IT management.\n\n"
            "• Marketing Brew: For professionals in marketing, advertising, branding, social media, and content creation.\n"
            "• CFO Brew: For senior leaders in finance, accounting, and strategy (e.g., CFO, VP of Finance, Controller, FP&A).\n"
            "• HR Brew: For professionals in human resources, talent acquisition, people operations, and employee development.\n"
            "• Retail Brew: For professionals in e-commerce, supply chain, merchandising, and CPG brand management.\n"
            "• Morning Brew (General): A fallback for broad business roles like Management Consultant, Business Analyst, or Chief of Staff that are relevant but don't fit a specific niche.\n"

            "**REQUIRED CONFIDENCE FIELDS:**\n"
            "- confidence_score: (float between 1-100)\n"
            "- reasoning_confidence: A short 2-3 line summary focusing on audience and industry fit."
            "\n\n"
            "--- ERROR HANDLING ---\n"
            "If a job description is empty, nonsensical, or you cannot extract the required fields for any reason, you MUST still return an object for that job. "
            "In this case, set all string and list fields to be empty, and all numeric fields to null. "
            "Set the 'confidence_score' to 0 and provide a brief explanation in the 'reasoning_confidence' field."
            "\n\n"
            "--- FINAL OUTPUT FORMAT ---\n"
        f"Your response **MUST** be a single, valid JSON array of objects. The order of jobs in your response array MUST exactly match the order of the input jobs. The array length must be exactly {len(jobs)}. Each object must correspond to a job and contain all the fields listed across the three tasks."
        )
        user_prompt = "--- JOBS TO PROCESS ---\n"
        for idx, job in enumerate(jobs):
            cleaned_description = clean_html(job.get('description', ''))
            user_prompt += f"{idx + 1}.\nTitle: {job.get('title', '')}\nCompany: {job.get('company_name', '')}\nDescription: {cleaned_description}\n\n"
        user_prompt += f"--- INDUSTRY HIERARCHY CONTEXT ---\n{json.dumps(industry_hierarchy)[:200000]}\n\n"
        user_prompt += "For each job provided, perform all three tasks and return the results in the specified JSON format."
        return system_prompt, user_prompt

    @classmethod
    def build_job_function_batch_prompt(cls, jobs, context=None):
        system_prompt = (
            "You are a highly precise AI model specializing in job function classification. Your sole purpose is to analyze a batch of job postings and assign the most accurate job function ID to each one from a shared list of possible functions.\n\n"
            "### Methodology\n"
            "1. **Analyze Job Details:** Read the job's title and description to understand its core responsibilities.\n"
            "2. **Reference the Shared List:** Compare the job's details against the single 'Job Functions List' provided.\n"
            "3. **Select the Best Fit:** Choose the single most specific and accurate job function ID from the list.\n"
            "4. **Justify Your Choice:** Provide a brief, single-sentence reasoning for your selection. **Your reasoning must be concise.**\n\n"
            "### IMPORTANT RULES\n"
            "- If no function is a good match, select the closest available option and briefly mention the mismatch in your reasoning.\n"
            "- **Never** include special characters, newlines, or complex formatting in the 'reasoning' string.\n"
            "- Your final output MUST be a valid JSON array of objects.\n"
            "\n"
            "### Output Format\n"
            f"Your final output MUST be a JSON array with an object for each job (exactly {len(jobs)} objects). Each object must contain: job_function_id, and reasoning.\n"
        )
        user_prompt = "Please classify the following jobs using the shared job function list below.\n\n--- JOBS TO PROCESS ---\n"
        for i, job in enumerate(jobs):
            cleaned_description = clean_html(job.get('description', ''))
            user_prompt += f"{i + 1}\nTitle: {job.get('title', '')}\nCompany: {job.get('company_name', '')}\nDescription: {cleaned_description}\n\n"
        if jobs and 'job_functions' in jobs[0]:
            user_prompt += f"--- JOB FUNCTIONS LIST (SHARED CONTEXT) ---\n{json.dumps(jobs[0].get('job_functions', []))}\n\n"
        user_prompt += "Select the most appropriate job function ID for each job."
        return system_prompt, user_prompt

    @classmethod
    def build_searchable_text_prompt(cls, jobs, context=None):
        system_prompt = (
            "You are an expert AI assistant that specializes in extracting keywords for search indexes. "
            "Your task is to analyze the provided job data and generate a single, unified string of search terms."
            "\n\n"
            "### RULES\n"
            "1. **Analyze All Inputs:** Consider the `description`, `ai_top_tags`, `ai_search_terms`, and `ai_skills` fields to get a complete understanding of the job.\n"
            "2. **Extract and Combine:** Extract all relevant keywords and search phrases from these inputs. Also create search term on your own based on you understanding.\n"
            "3. **De-duplicate:** Ensure the final string does not contain duplicate terms.\n"
            "4. **Single String Output:** Your output for each job MUST be a single string of these terms, separated by commas.\n"
            "5. **No Extra Text:** Do NOT include any introductory text, labels, field names, or explanations in the output string.\n\n"
            "### OUTPUT FORMAT\n"
            f"Your final response MUST be a single, valid JSON array of objects. The order of jobs in your response array MUST exactly match the order of the input jobs. The array length must be exactly {len(jobs)}."
        )

        user_prompt = "--- JOBS TO PROCESS ---\n"
        for idx, job in enumerate(jobs):
            # Combine all text-based fields for the AI to consider
            tags_str = ", ".join(job.get('ai_top_tags', []))
            terms_str = ", ".join(job.get('ai_search_terms', []))
            skills_str = ", ".join(job.get('ai_skills', []))
            cleaned_description = clean_html(job.get('description', ''))

            user_prompt += (
                f"{idx + 1}.\n"
                f"description: {cleaned_description}\n"
                f"ai_top_tags: {tags_str}\n"
                f"ai_search_terms: {terms_str}\n"
                f"ai_skills: {skills_str}\n\n"
            )

        user_prompt += "For each job provided, generate the comma-separated string of searchable text."
        return system_prompt, user_prompt

    async def generate_searchable_text_async(self, jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Generates a 'searchable_text' field for each job using a dedicated AI call.
        This follows the standard concurrent, cached, and resilient pattern.
        """
        if not jobs:
            return []

        tool = Tool(function_declarations=[
            FunctionDeclaration(name="create_searchable_text",
                                description="Creates a string of searchable keywords for a job.",
                                parameters=SEARCHABLE_TEXT_SCHEMA)])

        cache = await self._load_cache(self.SEARCHABLE_TEXT_CACHE_FILE)
        results, uncached_jobs = [], []

        for job in jobs:
            cache_key = job.get('job_hash')
            if cache_key in cache:
                job.update(cache[cache_key])
                results.append(job)
                if self.statistics: self.statistics.increment_cache_hit("searchable_text")
            else:
                uncached_jobs.append(job)

        if uncached_jobs:
            location_cycler = itertools.cycle(Config.GOOGLE_LOCATIONS)
            processed_results = await self._execute_batches_concurrently(
                uncached_jobs,
                self.build_searchable_text_prompt,
                tool,
                cache,
                lambda j: j.get('job_hash'),  # Use canonical hash for caching
                location_cycler
            )
            results.extend(processed_results)

        await self._save_cache(self.SEARCHABLE_TEXT_CACHE_FILE, cache)
        return results

    @staticmethod
    def build_job_function_lookup(job_functions_map: Dict[int, List[Dict[str, Any]]]) -> Dict[int, Dict[str, Any]]:
        lookup = {}
        if not job_functions_map: return lookup
        for functions_list in job_functions_map.values():
            if isinstance(functions_list, list):
                for func in functions_list:
                    if isinstance(func, dict) and 'id' in func:
                        lookup[func['id']] = func
        return lookup

    @staticmethod
    def _populate_industry_strings(jobs: List[Dict[str, Any]], job_function_lookup: Dict[int, Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not job_function_lookup:
            logger.warning("Job function lookup is empty; cannot populate industry strings.")
            return jobs
        for job in jobs:
            job['sector'], job['industry_group'], job['industry'] = '', '', ''
            if (job_function_id := job.get('job_function_id')) and (
            job_function_data := job_function_lookup.get(job_function_id)):
                try:
                    discipline_detail = job_function_data.get('discipline_detail', {})
                    sub_industry_detail = discipline_detail.get('sub_industry_detail', {})
                    industry_detail = sub_industry_detail.get('industry_detail', {})
                    industry_group_detail = industry_detail.get('industry_group_detail', {})
                    sector_detail = industry_group_detail.get('sector_detail', {})
                    job['sector'] = sector_detail.get('name', '')
                    job['industry_group'] = industry_group_detail.get('name', '')
                    job['industry'] = industry_detail.get('name', '')
                except Exception as e:
                    logger.error(f"Error parsing hierarchy for job function {job_function_id}: {e}")
        return jobs