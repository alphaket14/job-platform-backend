# src/ai_schemas.py

JOB_FUNCTION_SELECTION_SCHEMA = {
    "type": "object",
    "properties": {
        "selections": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "job_function_id": {"type": "integer"},
                    "reasoning": {"type": "string"},
                },
                "required": ["job_function_id", "reasoning"],
            },
        }
    },
    "required": ["selections"],
}



UNIFIED_INITIAL_ENRICHMENT_SCHEMA = {
    "type": "object",
    "properties": {
        "enriched_jobs": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    # Core Attributes
                    "ai_title": {"type": "string"},
                    "ai_description": {"type": "string"},
                    "ai_job_tasks": {"type": "array", "items": {"type": "string"}},
                    "ai_search_terms": {"type": "array", "items": {"type": "string"}},
                    "ai_top_tags": {"type": "array", "items": {"type": "string"}},
                    "ai_skills": {"type": "array", "items": {"type": "string"}},
                    "salary_min": {"type": "number"},
                    "salary_max": {"type": "number"},
                    "salary_period": {"type": "string"},
                    "currency": {"type": "string"},
                    # Industry Classification
                    "sector_id": {"type": "integer"},
                    "industry_group_id": {"type": "integer"},
                    "industry_id": {"type": "integer"},
                    "reasoning_industry": {"type": "string"},
                    # Confidence Score
                    "confidence_score": {"type": "number"},
                    "reasoning_confidence": {"type": "string"},
                },
                "required": [
                    "ai_title", "ai_description", "ai_skills","ai_search_terms","ai_job_tasks", "ai_top_tags",
                    "sector_id", "industry_group_id", "industry_id", "confidence_score"
                ],
            },
        }
    },
    "required": ["enriched_jobs"],
}

SEARCHABLE_TEXT_SCHEMA = {
    "type": "object",
    "properties": {
        "searchable_jobs": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "searchable_text": {"type": "string"}
                },
                "required": ["searchable_text"],
            },
        }
    },
    "required": ["searchable_jobs"],
}