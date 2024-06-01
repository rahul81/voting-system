from user_generator import Generator
from utils import file_writer

# defining number of candidates to generate
NUMBER_OF_CANDIDATES = 5
GENERATION_SEED = 'candidates'
NATIONALITY = 'US'

# randomuser.me generates user, below url will generate same user everytime as per given seed value and Nationality
CANDIDATES_GEN_API_URL = f"https://randomuser.me/api/?seed={GENERATION_SEED}&results={NUMBER_OF_CANDIDATES}&nat={NATIONALITY}"

DATA_PATH = './data'
OUTPUT_CSV_PATH = f"{DATA_PATH}/candidates.csv"

csv_header = [
    "title",
    "firstName",
    "lastName",
    "gender",
    "age",
    "city",
    "state",
    "country",
    "postal_code"
    ]


writer = file_writer(OUTPUT_CSV_PATH, 'w')

writer.writerow(csv_header)

candidates_generator = Generator()

candidates = candidates_generator.generate_user(
    api_url=CANDIDATES_GEN_API_URL
)

for candidate in candidates:
    writer.writerow(list(candidate.values()))


print(f"Successfully written data to {OUTPUT_CSV_PATH}")


