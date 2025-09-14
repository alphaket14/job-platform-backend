import re
import logging
from typing import List, Dict, Any
from rapidfuzz import fuzz
import json

from src.job_hasher import get_canonical_job_hash

logger = logging.getLogger(__name__)

def remove_location_mentions(description):
    """
    Remove location mentions from job descriptions for multi-location aggregation.
    This function is specifically for aggregation, not for job hashing.
    """
    if not description:
        return ""
    
    # Remove location patterns for aggregation purposes
    location_pattern = r"\b(?:in|at|located in|based in|from|work in|work at|work from|relocate to|relocation to|relocating to|location:?)\s+[A-Za-z .'-]+,?\s*[A-Z]{2,}(?:\s*-\s*\d{5})?(,?\s*(United States|USA|US|Canada|UK|United Kingdom|Australia|India|Germany|France|Spain|Italy|Singapore|Remote|Anywhere|Europe|Asia|Africa|America|Mexico|Brazil|Argentina|China|Japan|Korea|Russia|Turkey|UAE|Netherlands|Sweden|Norway|Finland|Denmark|Switzerland|Belgium|Ireland|Poland|Portugal|Greece|Czech Republic|Hungary|Romania|South Africa|Egypt|Nigeria|Kenya|Pakistan|Bangladesh|Indonesia|Malaysia|Philippines|Thailand|Vietnam|New Zealand|Chile|Colombia|Peru|Venezuela|Uruguay|Paraguay|Bolivia|Ecuador|Costa Rica|Panama|Guatemala|Honduras|El Salvador|Nicaragua|Cuba|Puerto Rico|Jamaica|Trinidad|Barbados|Bahamas|Dominican Republic|Haiti|Morocco|Algeria|Tunisia|Libya|Sudan|Ethiopia|Tanzania|Uganda|Ghana|Ivory Coast|Cameroon|Senegal|Zambia|Zimbabwe|Botswana|Namibia|Mozambique|Angola|Madagascar|Malawi|Mali|Niger|Burkina Faso|Benin|Rwanda|Burundi|Togo|Sierra Leone|Liberia|Congo|Gabon|Mauritius|Seychelles|Comoros|Lesotho|Swaziland|Somalia|Djibouti|Eritrea|South Sudan|Central African Republic|Chad|Guinea|Guinea-Bissau|Gambia|Cape Verde|Sao Tome|Equatorial Guinea|Western Sahara|Palestine|Israel|Lebanon|Jordan|Syria|Iraq|Iran|Afghanistan|Saudi Arabia|Qatar|Kuwait|Bahrain|Oman|Yemen|Armenia|Azerbaijan|Georgia|Kazakhstan|Uzbekistan|Turkmenistan|Kyrgyzstan|Tajikistan|Mongolia|Nepal|Bhutan|Sri Lanka|Maldives|Brunei|East Timor|Laos|Cambodia|Myanmar|North Korea|Taiwan|Hong Kong|Macau|Greenland|Iceland|Faroe Islands|Svalbard|Jan Mayen|Falkland Islands|French Guiana|Guadeloupe|Martinique|Reunion|Mayotte|New Caledonia|French Polynesia|Wallis and Futuna|Saint Pierre|Miquelon|Saint Barthelemy|Saint Martin|Saint Helena|Ascension|Tristan da Cunha|Anguilla|Bermuda|British Virgin Islands|Cayman Islands|Montserrat|Turks and Caicos Islands|Gibraltar|Pitcairn Islands|Saint Helena|Ascension|Tristan da Cunha|South Georgia|South Sandwich Islands|British Antarctic Territory|British Indian Ocean Territory|Antarctica|Other|Remote|Anywhere))?"
    
    # Remove location mentions
    cleaned = re.sub(location_pattern, "", description, flags=re.IGNORECASE)
    
    # Remove extra whitespace and normalize
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    
    return cleaned.lower()


def standardize_locations(raw_location):
    def loc_obj(city=None, state=None, country="US"):
        return {"city": city, "state": state, "country": country}

    if not raw_location:
        return [loc_obj()]

    # If already a list of dicts, just ensure keys and country
    if isinstance(raw_location, list):
        result = []
        for loc in raw_location:
            city = loc.get("city")
            state = loc.get("state")
            country = loc.get("country", "US")
            if country and country.lower() in ["united states", "usa", "us", "united states of america"]:
                country = "US"
            result.append(loc_obj(city, state, country))
        return result

    # If dict, wrap in list
    if isinstance(raw_location, dict):
        city = raw_location.get("city")
        state = raw_location.get("state")
        country = raw_location.get("country", "US")
        if country and country.lower() in ["united states", "usa", "us","united states of america"]:
            country = "US"
        return [loc_obj(city, state, country)]

    # If string, try to parse "City, ST, Country" or "City, ST"
    if isinstance(raw_location, str):
        parts = [p.strip() for p in raw_location.split(",")]
        if len(parts) == 3:
            city, state, country = parts
            if country.lower() in ["united states", "usa", "us","united states of america"]:
                country = "US"
            return [loc_obj(city=city, state=state, country=country)]
        elif len(parts) == 2:
            city, state = parts
            return [loc_obj(city=city, state=state, country="US")]
        elif len(parts) == 1:
            return [loc_obj(city=parts[0], state=None, country="US")]
        else:
            return [loc_obj()]
    return [loc_obj()]


def aggregate_multi_location_jobs_efficient(jobs, desc_threshold=85, accuracy_mode="balanced", buffer_percentage=0, **kwargs):
    """
    Ultra-fast multi-location job aggregation with configurable accuracy modes and buffer.
    
    Args:
        jobs: List of job dictionaries
        desc_threshold: Description similarity threshold (0-100) - default 80% for more lenient matching
        accuracy_mode: "fast", "balanced", or "accurate"
        buffer_percentage: Additional buffer percentage to add to thresholds (default 5%)
                          This allows jobs with (threshold - buffer)% similarity to be grouped
    """
    if not jobs:
        return [], 0
    
    # Apply buffer to make matching more lenient
    effective_threshold = desc_threshold - buffer_percentage
    
    # Configure thresholds based on accuracy mode
    if accuracy_mode == "fast":
        jaccard_threshold = 0.5  # Higher threshold = faster
        desc_threshold = 85 - buffer_percentage  # Apply buffer to fast mode
        group_thresholds = (30, 10, 3)  # Larger groups = less processing
        sampling_rate = 0.1      # 10% sampling for large groups
    elif accuracy_mode == "accurate":
        jaccard_threshold = 0.3  # Lower threshold = more accurate
        desc_threshold = 70 - buffer_percentage  # Apply buffer to accurate mode
        group_thresholds = (15, 8, 3)   # Smaller groups = more processing
        sampling_rate = 0.3      # 30% sampling for large groups
    else:  # balanced
        jaccard_threshold = 0.4  # Current setting
        desc_threshold = effective_threshold  # Use provided threshold with buffer
        group_thresholds = (20, 5, 3)    # Current setting
        sampling_rate = 0.2      # 20% sampling for large groups
    
    logger.info(f"Starting {accuracy_mode} multi-location aggregation for {len(jobs)} jobs")
    logger.info(f"Thresholds: Jaccard={jaccard_threshold}, Description={desc_threshold}% (with {buffer_percentage}% buffer), Sampling={sampling_rate}")

    
    def clean_description(desc):
        """Clean description for comparison."""
        if not desc:
            return ""
        return remove_location_mentions(desc).lower().strip()
    
    def get_word_set(desc_clean):
        """Get set of words for efficient comparison."""
        return set(desc_clean.split())
    
    def jaccard_similarity(set1, set2):
        """Calculate Jaccard similarity between two word sets."""
        if not set1 or not set2:
            return 0.0
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        return intersection / union if union > 0 else 0.0
    
    def fast_similarity(desc1, desc2):
        """Fast similarity using only token_sort_ratio."""
        clean1 = clean_description(desc1)
        clean2 = clean_description(desc2)
        return fuzz.token_sort_ratio(clean1, clean2) / 100.0
    
    # Step 1: Group jobs by key (title + company + employment_type) - O(n)
    initial_job_count = len(jobs)

    key_groups = {}

    for job in jobs:
        key = make_key(job, use_ai_title=kwargs.get('use_ai_title', False))
        if key not in key_groups:
            key_groups[key] = []
        key_groups[key].append(job)
    
    logger.info(f"Created {len(key_groups)} key groups")
    
    # Step 2: Process each group with adaptive strategy - O(n) total
    aggregated_jobs = []
    processed_groups = 0
    large_groups = 0
    medium_groups = 0
    small_groups = 0
    
    large_threshold, medium_threshold, small_threshold = group_thresholds
    
    for key, group_jobs in key_groups.items():
        processed_groups += 1
        if processed_groups % 200 == 0:  # Reduced logging frequency
            logger.info(f"Processed {processed_groups}/{len(key_groups)} groups... (Large: {large_groups}, Medium: {medium_groups}, Small: {small_groups})")
        
        if len(group_jobs) == 1:
            # Single job, no aggregation needed
            job = group_jobs[0].copy()
            job["locations"] = standardize_locations(job.get("locations"))
            job["is_multi_location"] = False
            aggregated_jobs.append(job)
            continue
        
        # Adaptive strategy based on group size and accuracy mode
        if len(group_jobs) > large_threshold:
            # Large groups: Use simple sampling with fast similarity
            large_groups += 1
            aggregated_group = _process_large_group_adaptive(group_jobs, desc_threshold, jaccard_threshold, sampling_rate)
        elif len(group_jobs) > medium_threshold:
            # Medium groups: Use fast Jaccard comparison
            medium_groups += 1
            aggregated_group = _process_medium_group_adaptive(group_jobs, desc_threshold, jaccard_threshold)
        else:
            # Small groups: Use direct comparison
            small_groups += 1
            aggregated_group = _process_small_group_adaptive(group_jobs, desc_threshold, jaccard_threshold)
        
        aggregated_jobs.extend(aggregated_group)
    
    logger.info(f"{accuracy_mode.capitalize()} aggregation completed: {len(jobs)} jobs -> {len(aggregated_jobs)} jobs")
    logger.info(f"Group statistics: Large groups: {large_groups}, Medium groups: {medium_groups}, Small groups: {small_groups}")
    duplicates_found = initial_job_count - len(aggregated_jobs)

    return aggregated_jobs, duplicates_found


def _process_large_group_adaptive(group_jobs, desc_threshold, jaccard_threshold, sampling_rate):
    """Process large groups using adaptive sampling for configurable speed/accuracy."""
    def clean_description(desc):
        if not desc:
            return ""
        return remove_location_mentions(desc).lower().strip()
    
    def get_word_set(desc_clean):
        return set(desc_clean.split())
    
    def jaccard_similarity(set1, set2):
        if not set1 or not set2:
            return 0.0
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        return intersection / union if union > 0 else 0.0
    
    def fast_similarity(desc1, desc2):
        clean1 = clean_description(desc1)
        clean2 = clean_description(desc2)
        return fuzz.token_sort_ratio(clean1, clean2) / 100.0
    
    # Step 1: Adaptive sampling based on accuracy mode
    sample_size = max(int(len(group_jobs) * sampling_rate), 5)
    if sample_size >= len(group_jobs):
        sample_size = len(group_jobs)  # Use all if too small
    
    import random
    sample_indices = random.sample(range(len(group_jobs)), sample_size)
    sample_jobs = [group_jobs[i] for i in sample_indices]
    
    # Step 2: Pre-process sample jobs
    sample_word_sets = []
    for job in sample_jobs:
        desc_clean = clean_description(job.get("description", ""))
        word_set = get_word_set(desc_clean)
        sample_word_sets.append((job, word_set))
    
    # Step 3: Use union-find with adaptive thresholds
    class UnionFind:
        def __init__(self, n):
            self.parent = list(range(n))
            self.rank = [0] * n
        
        def find(self, x):
            if self.parent[x] != x:
                self.parent[x] = self.find(self.parent[x])
            return self.parent[x]
        
        def union(self, x, y):
            px, py = self.find(x), self.find(y)
            if px != py:
                if self.rank[px] < self.rank[py]:
                    px, py = py, px
                self.parent[py] = px
                if self.rank[px] == self.rank[py]:
                    self.rank[px] += 1
    
    uf = UnionFind(len(sample_jobs))
    
    # Step 4: Compare sample jobs with adaptive thresholds
    for i in range(len(sample_jobs)):
        for j in range(i + 1, len(sample_jobs)):
            jaccard_sim = jaccard_similarity(sample_word_sets[i][1], sample_word_sets[j][1])
            if jaccard_sim >= jaccard_threshold:
                fast_sim = fast_similarity(
                    sample_jobs[i].get("description", ""),
                    sample_jobs[j].get("description", "")
                )
                if fast_sim >= desc_threshold / 100.0:
                    uf.union(i, j)
    
    # Step 5: Group sample jobs
    from collections import defaultdict
    groups = defaultdict(list)
    for i in range(len(sample_jobs)):
        root = uf.find(i)
        groups[root].append(sample_jobs[i])
    
    # Step 6: Assign remaining jobs to closest group
    remaining_jobs = [job for i, job in enumerate(group_jobs) if i not in sample_indices]
    
    for job in remaining_jobs:
        best_group = None
        best_similarity = 0
        
        for group in groups.values():
            # Compare with first job in group
            group_job = group[0]
            fast_sim = fast_similarity(
                job.get("description", ""),
                group_job.get("description", "")
            )
            
            if fast_sim >= desc_threshold / 100.0 and fast_sim > best_similarity:
                best_similarity = fast_sim
                best_group = group
        
        if best_group:
            best_group.append(job)
        else:
            # Create new group
            groups[len(groups)] = [job]
    
    # Step 7: Create aggregated jobs
    return _create_aggregated_jobs_from_groups(groups.values())

def _process_medium_group_adaptive(group_jobs, desc_threshold, jaccard_threshold):
    """Process medium groups using adaptive Jaccard comparison."""
    def clean_description(desc):
        if not desc:
            return ""
        return remove_location_mentions(desc).lower().strip()
    
    def get_word_set(desc_clean):
        return set(desc_clean.split())
    
    def jaccard_similarity(set1, set2):
        if not set1 or not set2:
            return 0.0
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        return intersection / union if union > 0 else 0.0
    
    def fast_similarity(desc1, desc2):
        clean1 = clean_description(desc1)
        clean2 = clean_description(desc2)
        return fuzz.token_sort_ratio(clean1, clean2) / 100.0
    
    # Pre-process all jobs
    job_word_sets = []
    for job in group_jobs:
        desc_clean = clean_description(job.get("description", ""))
        word_set = get_word_set(desc_clean)
        job_word_sets.append((job, word_set))
    
    # Use union-find with adaptive thresholds
    class UnionFind:
        def __init__(self, n):
            self.parent = list(range(n))
            self.rank = [0] * n
        
        def find(self, x):
            if self.parent[x] != x:
                self.parent[x] = self.find(self.parent[x])
            return self.parent[x]
        
        def union(self, x, y):
            px, py = self.find(x), self.find(y)
            if px != py:
                if self.rank[px] < self.rank[py]:
                    px, py = py, px
                self.parent[py] = px
                if self.rank[px] == self.rank[py]:
                    self.rank[px] += 1
    
    uf = UnionFind(len(group_jobs))
    
    # Compare all pairs with adaptive thresholds
    for i in range(len(group_jobs)):
        for j in range(i + 1, len(group_jobs)):
            jaccard_sim = jaccard_similarity(job_word_sets[i][1], job_word_sets[j][1])
            if jaccard_sim >= jaccard_threshold:
                fast_sim = fast_similarity(
                    group_jobs[i].get("description", ""),
                    group_jobs[j].get("description", "")
                )
                if fast_sim >= desc_threshold / 100.0:
                    uf.union(i, j)
    
    # Group jobs
    from collections import defaultdict
    groups = defaultdict(list)
    for i in range(len(group_jobs)):
        root = uf.find(i)
        groups[root].append(group_jobs[i])
    
    return _create_aggregated_jobs_from_groups(groups.values())

def _process_small_group_adaptive(group_jobs, desc_threshold, jaccard_threshold):
    """Process small groups using adaptive direct comparison."""
    def clean_description(desc):
        if not desc:
            return ""
        return remove_location_mentions(desc).lower().strip()
    
    def get_word_set(desc_clean):
        return set(desc_clean.split())
    
    def jaccard_similarity(set1, set2):
        if not set1 or not set2:
            return 0.0
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        return intersection / union if union > 0 else 0.0
    
    def fast_similarity(desc1, desc2):
        clean1 = clean_description(desc1)
        clean2 = clean_description(desc2)
        return fuzz.token_sort_ratio(clean1, clean2) / 100.0
    
    # Pre-process descriptions
    job_word_sets = []
    for job in group_jobs:
        desc_clean = clean_description(job.get("description", ""))
        word_set = get_word_set(desc_clean)
        job_word_sets.append((job, word_set))
    
    # Use union-find with adaptive thresholds
    class UnionFind:
        def __init__(self, n):
            self.parent = list(range(n))
            self.rank = [0] * n
        
        def find(self, x):
            if self.parent[x] != x:
                self.parent[x] = self.find(self.parent[x])
            return self.parent[x]
        
        def union(self, x, y):
            px, py = self.find(x), self.find(y)
            if px != py:
                if self.rank[px] < self.rank[py]:
                    px, py = py, px
                self.parent[py] = px
                if self.rank[px] == self.rank[py]:
                    self.rank[px] += 1
    
    uf = UnionFind(len(group_jobs))
    
    # Compare all pairs with adaptive thresholds
    for i in range(len(group_jobs)):
        for j in range(i + 1, len(group_jobs)):
            jaccard_sim = jaccard_similarity(job_word_sets[i][1], job_word_sets[j][1])
            if jaccard_sim >= jaccard_threshold:
                fast_sim = fast_similarity(
                    group_jobs[i].get("description", ""),
                    group_jobs[j].get("description", "")
                )
                if fast_sim >= desc_threshold / 100.0:
                    uf.union(i, j)
    
    # Group jobs
    from collections import defaultdict
    groups = defaultdict(list)
    for i in range(len(group_jobs)):
        root = uf.find(i)
        groups[root].append(group_jobs[i])
    
    return _create_aggregated_jobs_from_groups(groups.values())


def _create_aggregated_jobs_from_groups(groups):
    """
    Helper function to create aggregated jobs from groups.
    This version correctly merges jobs into a single record with a stable, combined hash.
    """
    aggregated_jobs = []

    for group in groups:
        # If a group only has one job, it's not a duplicate.
        # Leave it as is, keeping its original unique hash.
        if len(group) == 1:
            job = group[0].copy()
            job["locations"] = standardize_locations(job.get("locations"))
            job["is_multi_location"] = False
            aggregated_jobs.append(job)

        # If a group has multiple jobs, they must be merged.
        else:
            # 1. Take the first job as the base for all data.
            sorted_group = sorted(group, key=lambda j: j.get('job_hash', ''))
            base_job = sorted_group[0].copy()

            # 2. Merge locations from all jobs in the group.
            all_locations = set()
            for job in sorted_group:
                for loc in standardize_locations(job.get('locations')):
                    loc_key = f"{loc.get('city', '')}|{loc.get('state', '')}|{loc.get('country', '')}"
                    all_locations.add(loc_key)

            merged_locations = []
            for loc_key in sorted(list(all_locations)):
                city, state, country = loc_key.split('|')
                merged_locations.append({"city": city or None, "state": state or None, "country": country or "US"})

            base_job['locations'] = merged_locations
            base_job['is_multi_location'] = len(merged_locations) > 1

            # 3. After modifying the content, generate a new hash based on its FINAL content.
            base_job['job_hash'] = get_canonical_job_hash(base_job)

            aggregated_jobs.append(base_job)

    return aggregated_jobs

def make_key(job, use_ai_title=False):
    """Create a hash key for job grouping."""
    title_key = 'ai_title' if use_ai_title and job.get('ai_title') else 'title'
    title = (job.get(title_key) or "").lower().strip()
    company = (job.get("company_name") or "").lower().strip()
    employment_type = (job.get("employment_type") or "").lower().strip()
    return f"{title}|{company}|{employment_type}"

