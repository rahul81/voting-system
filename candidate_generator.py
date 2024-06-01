from user_generator import Generator
from utils import file_writer
import random

# defining number of candidates to generate
NUMBER_OF_CANDIDATES = 5
GENERATION_SEED = 'candidates'
NATIONALITY = 'US'

PARTIES = ['party' + str(i) for i in range(1,6)]
PARTY_SEED = 22

random.seed(PARTY_SEED)

# randomuser.me generates user, below url will generate same user everytime as per given seed value and Nationality
CANDIDATES_GEN_API_URL = f"https://randomuser.me/api/?seed={GENERATION_SEED}&results={NUMBER_OF_CANDIDATES}&nat={NATIONALITY}"

DATA_PATH = './data'
OUTPUT_CSV_PATH = f"{DATA_PATH}/candidates.csv"

csv_header = [
    "candidate_id",
    "candidate_name",
    "candidate_gender",
    "candidate_age",
    "party",
    "photo_url"
    ]


writer = file_writer(OUTPUT_CSV_PATH, 'w')

writer.writerow(csv_header)

candidates_generator = Generator()

candidates = candidates_generator.generate_user(
    api_url=CANDIDATES_GEN_API_URL
)

for candidate in candidates:

    chosen_party = random.choice(PARTIES) # choose a random party for a candidate

    data = [
        candidate['id'],
        candidate['title'] + ' ' + candidate['firstName'] + ' ' + candidate['lastName'],
        candidate['gender'],
        candidate['age'],
        chosen_party,
        candidate['photo_url']
    ]
    writer.writerow(data)

    # remove the party for once it is picked already, so that it won't be assigned to other candidates
    PARTIES.remove(chosen_party)


print(f"Successfully written data to {OUTPUT_CSV_PATH}")


