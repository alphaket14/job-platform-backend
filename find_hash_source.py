#!/usr/bin/env python3
"""
An advanced script to test combinations of job data to find the source of a given hash.
It tests different field combinations and description cleaning/truncation levels.
"""
import argparse
import base64
import hashlib
import logging
import re
from itertools import product

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# --- Hashing Logic (copied from job_hasher.py for standalone use) ---
def clean_description_for_hashing(description: str) -> str:
    if not description: return ""
    cleaned = re.sub(r'<[^>]+>', ' ', description)
    cleaned = re.sub(r'https?://\S+', ' ', cleaned)
    cleaned = re.sub(r'[^a-zA-Z0-9\s]', ' ', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned.lower()


def get_hash(canonical_string: str) -> str:
    hasher = hashlib.sha256(canonical_string.encode('utf-8'))
    return base64.b64encode(hasher.digest()).decode('utf-8')


# --- Data from the three corrupted jobs ---
TITLES = {"Respiratory Therapist"}
COMPANIES = {"Saint Luke's Hospital of Kansas City", "Apria"}
EMPLOYMENT_TYPES = {"Full-time", "Part-time", "FULL_TIME", "PART_TIME"}
DESCRIPTIONS = {
    "<b> <b>Job Description</b> </b><br><br><b><b> <b><b>Exciting things are happening! Are you a Respiratory Therapist looking to join Missouri's largest healthcare organization? Look no further!</b></b> </b></b><ul><li>Full Time Nights</li><li>3-12hr shifts per week; 6:30pm-7:00am</li><li>Rotating weekends and holidays</li><li>CRT required, RRT preferred</li><li>MO RT License</li><li>BLS preferred</li><li>Career Advancement Program</li></ul><b>The Opportunity</b>:<br><br>Hedrick Medical Center in Chillicothe, MO is seeking a Respiratory Therapist. To be successful in this role, you will need to be able to work effectively and collaboratively with nursing, physicians and other team members. This position requires strong communication and organizational skills so you can build patient rapport easily.<br><br><b>Why Saint Luke's?</b><br><ul><li>We believe in work/life balance.</li><li>We are dedicated to innovation and always looking for ways to improve.</li><li>We believe in creating a collaborative environment where all voices are heard.</li><li>We are here for you and will support you in achieving your goals.</li></ul>#LI-CK2<br><br><b> <b>Job Requirements</b> </b><br><br>Applicable Experience:<br>Less than 1 year<br><br>Basic Life Support - American Heart Association or Red Cross, Licensed Respiratory Therapist (MO) - Missouri Division of Professional Registration, Reg Respiratory Therapist (NBRC) - The National Board for Respiratory Care<br><br>Associate Degree<br><br><b> <b>Job Details</b> </b><br>Full Time<br><br>Night (United States of America)<br><br><b> The best place to get care. The best place to give care . Saint Luke's 12,000 employees strive toward that vision every day. Our employees are proud to work for the only faith-based, nonprofit, locally owned health system in Kansas City. Joining Saint Luke's means joining a team of exceptional professionals who strive for excellence in patient care. Do the best work of your career within a highly diverse and inclusive workspace where all voices matter.</b><br><br><b>Join the Kansas City region's premiere provider of health services. Equal Opportunity Employer. </b><p>By applying, you consent to your information being transmitted by Veritone to the Employer, as data controller, through the Employer’s data processor SonicJobs.<br>See SonicJobs Privacy Policy at https://www.sonicjobs.com/us/privacy-policy and Terms of Use at https://www.sonicjobs.com/us/terms-conditions</p>PandoLogic. Keywords: Respiratory Therapist, Location: Kansas City, MO - 64106"
    "<p>At Owens & Minor, we are a critical part of the healthcare process. As a Fortune 500 company with 350+ facilities across the US and 22,000 teammates in over 90 countries, we provide integrated technologies, products and services across the full continuum of care. Customers—and their patients—are at the heart of what we do.</p><p>Our mission is to empower our customers to advance healthcare, and our success starts with our teammates.</p><p><br></p><p><strong>Owens & Minor Teammate Benefits Include:</strong></p><ul><li>Medical, dental, and vision insurance, available on first working day</li><li>401(k), eligibility after one year of service</li><li>Employee stock purchase plan</li><li>Tuition reimbursement</li></ul><p><br></p><p><strong>ABOUT THE COMPANY</strong></p><p><em>Apria Healthcare’s mission is to improve the quality of life for our patients at home. We are looking for empathetic, thoughtful, and compassionate people, to meet the needs of our patients. Already an industry leader in healthcare services, we provide home respiratory services and select medical equipment to help our patients sleep better, breathe better, heal faster, and thrive longer.</em></p><p>$37.00 - $45.00 / Hour</p><p><br></p><p><strong>JOB SUMMARY</strong></p><p>Assists in the treatment and management of patients with clinical needs.</p><p><br></p><p><strong>ESSENTIAL DUTIES AND RESPONSIBILITIES</strong></p><ul><li>Provides instruction to patients and/or caregivers on the proper use of equipment and/or respiratory care procedures.</li><li>Ensures patients and/or caregivers can effectively operate and maintain equipment. Performs clinical assessments and tests such as pulse oximetry, ETCO2, spirometry, and vital signs.</li><li>Responsible for routine patient follow-up contacts based on individual needs.</li><li>May need to perform on-call duties as needed.</li><li>Provides and operates various types of respiratory care equipment including but not limited to oxygen therapy, nebulization therapy, apnea monitoring, suctioning, PAP, invasive and non-invasive ventilation.</li><li>Inspects and tests equipment to ensure proper operating condition.</li><li>Prepares and maintains a record for each patient containing all pertinent information, care plans, physician prescriptions and follow-up documentation.</li><li>Responsible for accurate data entry on monitoring websites for certain respiratory equipment</li><li>Consults with referring physician regarding patient treatment, medical condition, home environment, and Plan of Care.</li><li>Participates in ongoing education and training sessions regarding respiratory patient care.</li><li>Assist with patient scheduling as needed.</li><li>Performs other duties as required.</li></ul><p><strong>SUPERVISORY RESPONSIBILITIES</strong></p><ul><li>N/A</li></ul><p><strong>Minimum Required Qualifications</strong></p><ul><li>Meets company minimum standard of Background Check</li></ul><p><strong>Education And/or Experience</strong></p><ul><li>Graduate of an accredited program for respiratory therapy is required.</li></ul><p><strong>Certificates, Licenses, Registrations or Professional Designations</strong></p><ul><li>Must possess a valid and current driver’s license and auto insurance per Apria policy. Will be required to drive personal vehicle for patient home visits.</li><li>Registration or certification by the National Board for Respiratory Care (NBRC).</li><li>Hold a current RCP license in the state of practice (or states that the location covers) if that state requires an RCP license to allow the practice of respiratory therapy.</li><li>Hold a current CPR Certification.</li><li>Hold all applicable licensure in good standing for all states of practice.</li></ul><p><strong>SKILLS, KNOWLEDGE AND ABILITIES</strong></p><ul><li>Strong interpersonal and teamwork skills.</li><li>Ability to multi-task effectively.</li><li>Ability to communicate effectively in person, on the phone and electronically</li><li>Successful completion of Apria's respiratory therapy orientation and competency evaluation program.</li></ul><p><strong>Computer Skills</strong></p><ul><li>Ability to use electronic hand held device</li><li>Microsoft Office programs</li><li>Basic printing/faxing/scanning</li></ul><p><strong>Language Skills</strong></p><ul><li>English (reading, writing, verbal)</li></ul><p><strong>PREFERRED QUALIFICATIONS</strong></p><p><strong>Education and/or Experience</strong></p><ul><li>At least one year related experience is preferred.</li></ul><p><strong>Computer Skills</strong></p><p><strong>SKILLS, KNOWLEDGE AND ABILITIES</strong></p><p><strong>Language Skills</strong></p><ul><li>Bilingual (reading, writing, verbal)</li></ul><p><strong>PHYSICAL DEMANDS</strong></p><p>While performing the duties of this job, the employee uses his/her hands to finger, handle or feel objects, tools or controls; reach with hands and arms; stoop, kneel, or crouch; talk or hear. The employee uses computer and telephone equipment. Specific vision requirements of this job include close vision and distance vision. Must be able to travel by plane and automobile (if applicable). Possible lifting of equipment up to 50 lbs.</p><p><strong>WORK ENVIRONMENT</strong></p><p>The work environment characteristics described here are representative of those an employee encounters while performing the essential functions of this job. Reasonable accommodations may be made to enable individuals with disabilities to perform the essential functions.</p><ul><li>The employee is required to safely operate a motor vehicle during the day and night and in a wide range of weather and traffic conditions.</li><li>The noise level in the work environment is varies based on the locations or activities proximate to which can range from low to high.</li><li>There is moderate exposure to dust, fume, mists and odors.</li><li>Temperature ranges from normal indoor climate-controlled environment in buildings or vehicles and various outdoor conditions and temperature extremes encountered throughout the year in a variety of US states.</li><li>General lighting is generally provided via florescent lighting indoors, and natural lighting outdoors, and low light conditions consistent with outdoor and/or night working environment.</li><li>During off site travel events the employee may be exposed to higher noise levels requiring the use of hearing protection, with moderate potential exposure to moderate dust, chemicals, fumes and odors, as well as cryogenic and cleaning agents.</li><li>During off site travel events the employee may be required to ride in company delivery vehicles and accompany employees on deliveries and enter into patient homes.</li><li>May be required to receive vaccinations and participate in medical assessments and testing consistent with the work environment or patients exposed to.</li><li>Will be required to wear various personal protective equipment consistent with the hazards encountered in this role.</li><li>Will be required to use hand tools for assembly and repair, material handling equipment, cutting, carrying devices, cleanup kits or equipment.</li><li>May be required to work with cryogenic fluids requiring special precautions and PPE.</li></ul><p><strong>The physical demands and work environment characteristics described above are representative of those an employee encounters while performing the essential functions of this job. Reasonable accommodations may be made to enable individuals with disabilities to perform the essential functions.</strong></p><p>If you feel this opportunity could be the next step in your career, we encourage you to apply. This position will accept applications on an ongoing basis.</p><p>Owens & Minor is an Equal Opportunity Employer. All qualified applicants will receive consideration for employment without regard to race, color, national origin, sex, sexual orientation, genetic information, religion, disability, age, status as a veteran, or any other status prohibited by applicable national, federal, state or local law.</p>",
    "<b> <b>Job Description</b> </b><br><br><b><b> <b>Exciting things are happening! Are you a Respiratory Therapist looking to join Missouri's largest healthcare organization? Look no further!</b> </b></b><ul><li>Full Time Nights</li><li>3-12hr shifts per week; 6:45pm-7:15am</li><li>Rotating weekends and holidays</li><li>CRT required, RRT preferred</li><li>MO RT License</li><li>NICU preferred</li><li>BLS preferred</li><li>Career Advancement Program</li></ul><b>The Opportunity</b>:<br><br>Saint Luke's Hospital on the Plaza is seeking a Respiratory Therapist. To be successful in this role, you will need to be able to work effectively and collaboratively with nursing, physicians and other team members. This position requires strong communication and organizational skills so you can build patient rapport easily.<br><br><b>Why Saint Luke's?</b><br><ul><li>We believe in work/life balance.</li><li>We are dedicated to innovation and always looking for ways to improve.</li><li>We believe in creating a collaborative environment where all voices are heard.</li><li>We are here for you and will support you in achieving your goals.</li></ul>#LI-CK2<br><br><b> <b>Job Requirements</b> </b><br><br>Applicable Experience:<br>Less than 1 year<br><br>Basic Life Support - American Heart Association or Red Cross, Reg Respiratory Therapist (NBRC) - The National Board for Respiratory Care, Respiratory Care Practitioner (MO) - Missouri Division of Professional Registration<br><br>Associate Degree<br><br><b> <b>Job Details</b> </b><br>Full Time<br><br>Night (United States of America)<br><br><b> The best place to get care. The best place to give care . Saint Luke's 12,000 employees strive toward that vision every day. Our employees are proud to work for the only faith-based, nonprofit, locally owned health system in Kansas City. Joining Saint Luke's means joining a team of exceptional professionals who strive for excellence in patient care. Do the best work of your career within a highly diverse and inclusive workspace where all voices matter.</b><br><br><b>Join the Kansas City region's premiere provider of health services. Equal Opportunity Employer. </b>"
}


def check_combo(combo_tuple, target_hash):
    """Calculates and checks the hash for a given combination tuple."""
    canonical_string = "|".join(combo_tuple)
    calculated_hash = get_hash(canonical_string)

    if calculated_hash == target_hash:
        logger.info("✅ MATCH FOUND!")
        print("-" * 80)
        print(f"Target Hash: {target_hash}")
        print("\nThis hash was generated from the following canonical string:")
        print(canonical_string)
        print("\nWhich is composed of:")

        # Unpack the tuple for printing
        title, company, emp_type, desc = combo_tuple
        print(f"  - Title          : '{title}'")
        print(f"  - Company        : '{company}'")
        print(f"  - Employment Type: '{emp_type}'")
        print(f"  - Description    : '{desc[:100]}...'")
        print("-" * 80)
        return True
    return False


def main():
    parser = argparse.ArgumentParser(description='Find the source string for a given job hash.')
    parser.add_argument('target_hash', type=str, help='The corrupted hash value to find.')
    args = parser.parse_args()

    target_hash = args.target_hash
    logger.info(f"Searching for canonical string that produces hash: {target_hash}")

    # Create pools of data to test
    title_pool = list(TITLES)
    company_pool = list(COMPANIES)
    employment_type_pool = list(EMPLOYMENT_TYPES)
    description_pool = list(DESCRIPTIONS)

    # --- Test different field combinations ---
    # Hashing logic might have been simpler, e.g., only using title and company

    # Test 1: Title only
    logger.info("Testing combinations with Title only...")
    for title in title_pool:
        if check_combo((title, "", "", ""), target_hash): return

    # Test 2: Title and Company
    logger.info("Testing combinations with Title and Company...")
    for combo in product(title_pool, company_pool):
        if check_combo((combo[0], combo[1], "", ""), target_hash): return

    # Test 3: Title, Company, and Employment Type
    logger.info("Testing combinations with Title, Company, and Employment Type...")
    for combo in product(title_pool, company_pool, employment_type_pool):
        if check_combo((combo[0], combo[1], combo[2], ""), target_hash): return

    # Test 4: The full combination, but with various description cleanings and truncations
    logger.info("Testing full combinations with various description formats...")
    for combo in product(title_pool, company_pool, employment_type_pool, description_pool):
        title, company, emp_type, desc = combo

        # Test with fully cleaned description
        if check_combo((title, company, emp_type, clean_description_for_hashing(desc)), target_hash): return

        # Test with raw (uncleaned) description
        if check_combo((title, company, emp_type, desc), target_hash): return

        # Test with truncated versions
        for length in [50, 100, 200, 500, 1000]:
            if check_combo((title, company, emp_type, clean_description_for_hashing(desc)[:length]),
                           target_hash): return
            if check_combo((title, company, emp_type, desc[:length]), target_hash): return

    logger.warning("No match found after testing all combinations.")


if __name__ == "__main__":
    main()